package fr.eurecom.hybris;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.kvs.CloudProvider;
import fr.eurecom.hybris.kvs.KvStore;
import fr.eurecom.hybris.mds.MdStore;
import fr.eurecom.hybris.mds.Metadata;
import fr.eurecom.hybris.mds.Metadata.Timestamp;

public class Hybris {

    private Config conf = Config.getInstance();
    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);
    
    private MdStore mds;
    private KvStore kvs;
    
    private int quorum;
    
    public Hybris() throws HybrisException {
        try {
            mds = new MdStore(conf.getProperty(Config.ZK_ADDR), 
                                conf.getProperty(Config.ZK_ROOT));
        } catch (IOException e) {
            logger.error("Could not initialize the Zookeeper metadata store.", e);
            throw new HybrisException("Could not initialize the Zookeeper metadata store.");
        }
        
        kvs = new KvStore(conf.getProperty(Config.KVS_ROOT), 
                            Boolean.parseBoolean(conf.getProperty(Config.KVS_TESTSONSTARTUP)));
        
        int t = Integer.parseInt(conf.getProperty(Config.CONST_T));
        this.quorum = t + 1;
    }
    
   
    /* ---------------------------------------------------------------------------------------
                                            Public APIs
       --------------------------------------------------------------------------------------- */


    public void write(String key, byte[] value) throws HybrisException {
        
        Timestamp ts;
        byte[] rawMetadataValue = mds.tsRead(key);
        if (rawMetadataValue == null) 
            ts = new Timestamp(0, Utils.getClientId());
        else {
            ts = new Metadata(rawMetadataValue).getTs();
            ts.inc( Utils.getClientId() );
        }
        
        List<CloudProvider> savedReplicasLst = new ArrayList<CloudProvider>();
        String kvsKey = Utils.getKvsKey(key, ts);
        
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders())
                try {                                           // TODO parallelize
                    kvs.put(provider, kvsKey, value);                    
                    savedReplicasLst.add(provider);
                    if (savedReplicasLst.size() >= this.quorum) break;
                } catch (Exception e) {
                    logger.warn("Error while trying to store " + key + " on " + provider.getId(), e);
                }
        }
        
        if (savedReplicasLst.size() < this.quorum) {
            kvsGc(kvsKey, savedReplicasLst);                    // TODO make asynchronous
            logger.warn("Hybris could not manage to store data in cloud stores for key {}.", key);
            throw new HybrisException("Hybris could not manage to store data in cloud stores");
        } 
               
        try {
            mds.tsWrite(key, new Metadata(ts, Utils.getHash(value), savedReplicasLst));
        } catch (HybrisException e) {
            kvsGc(kvsKey, savedReplicasLst);                    // TODO make asynchronous
            logger.warn("Hybris could not manage to store metadata on Zookeeper for key {}.", key);
            throw new HybrisException("Hybris could not manage to store the metadata on Zookeeper");
        }
        
        StringBuilder strBld = new StringBuilder("Data successfully stored to these replicas: ");
        for (CloudProvider cloud : savedReplicasLst) strBld.append(cloud.getId() + " ");
        logger.info(strBld.toString());
    }
    

    public byte[] read(String key) throws HybrisException {
        
        byte[] rawMetadataValue = mds.tsRead(key);
        if (rawMetadataValue == null) {
            logger.warn("Hybris could not find metadata associated with key {}.", key);
            return null;
        }
        
        Metadata tsdir = new Metadata(rawMetadataValue);
        byte[] value = null;
        String kvsKey = Utils.getKvsKey(key, tsdir.getTs());
        
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders()) {
                
                if (!tsdir.getReplicasLst().contains(provider)) 
                    continue;
                
                try {
                    value = kvs.get(provider, kvsKey);
                } catch (IOException e) {
                    logger.warn("Error while trying to retrieve " + key + " from " + provider.getId(), e);
                    continue;
                }
                
                if (value != null) {
                    if (Arrays.equals(tsdir.getHash(), Utils.getHash(value))) {
                        logger.info("Value successfully retrieved from provider {}", provider.getId());
                        return value;
                    } else      // the hash doesn't match: byzantine fault: let's try with the other ones
                        continue;
                } else {        // this could be due to:
                                // a. byzantine replicas 
                                // b. concurrent gc 
                    byte[] newRawMetadataValue = mds.tsRead(key);
                    if (newRawMetadataValue != null) {
                        Metadata newtsdir = new Metadata(newRawMetadataValue);
                        if (newtsdir.getTs().isGreater(tsdir.getTs())) {    // it's because of concurrent gc
                            logger.warn("Could not get the value of {} from replica {} because of concurrent gc. Restarting read.", 
                                        key, provider.getId());
                            return read(key);                               // trigger recursive read
                        } else
                            continue;                                       // otherwise it's because of b.: let's try with the other ones
                    } else {                                                // the value does not exist anymore because of concurrent gc
                        logger.warn("Hybris could not find the metadata associated with key {}.", key);
                        return null;
                    }
                }
            }
        }
        
        logger.warn("Hybris could not manage to retrieve the data associated with key {} from cloud stores.", key);
        return null;
    }

    
    public void gc(String key) throws HybrisException {
        
        byte[] rawMetadataValue = mds.tsRead(key);
        if (rawMetadataValue == null) {
            logger.debug("Hybris could not find the metadata associated with key {}.", key);
            return;
        }
        
        Metadata tsdir = new Metadata(rawMetadataValue);
        String kvsKey = Utils.getKvsKey(key, tsdir.getTs());
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders()) {
                
                if (!tsdir.getReplicasLst().contains(provider)) 
                    continue;
                
                try {
                    kvs.delete(provider, kvsKey);  // XXX what about the previous versions?
                } catch (IOException e) {
                    logger.warn("error while deleting {} from {}.", key, provider.getId());
                }
            }
        }
        
        mds.delete(key);
    }
    
    
    /* --------------------------------------------------------------------------------------- 
                                       Private methods
       --------------------------------------------------------------------------------------- */
    
    private void kvsGc(String kvsKey, List<CloudProvider> providers) {
        
        for (CloudProvider cloud : providers)
            try {
                kvs.delete(cloud, kvsKey);
            } catch (IOException e) {
                logger.warn("error while deleting {} from {}.", kvsKey, cloud.getId());
            }
    }
    
    
    /**
     * TODO TEMP for debugging purposes
     * @throws IOException 
     * @throws HybrisException 
     */
    public static void main(String[] args) throws HybrisException {
        Hybris hybris = new Hybris();
        hybris.write("mykey", "my_value".getBytes());
        String value = new String(hybris.read("mykey"));
        System.out.println("Read output: " + value);
    }
}