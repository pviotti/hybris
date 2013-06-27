package fr.eurecom.hybris;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.kvs.KvStore;
import fr.eurecom.hybris.mds.MdStore;
import fr.eurecom.hybris.mds.Metadata;
import fr.eurecom.hybris.mds.Metadata.Timestamp;

public class Hybris {

    private Config conf = Config.getInstance();
    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);
    
    private MdStore mds;
    private KvStore kvs;
    
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
    }
    
   
    // =======================================================================================
    //                                      PUBLIC APIs
    // ---------------------------------------------------------------------------------------
    
    public byte[] read(String key) throws HybrisException {
        
        byte[] rawMetadataValue = mds.tsRead(key);
        if (rawMetadataValue == null) {
            logger.warn("Hybris could not find the metadata associated with key {}.", key);
            return null;
        }
        
        Metadata tsdir = new Metadata(rawMetadataValue);
        byte[] value = null; 
        for (String replica : tsdir.getReplicasLst()){     // for each replica
            
            value = kvs.getFromCloud(replica, Utils.getKvsKey(key, tsdir.getTs()));
            
            if (value != null) {
                
                if (Arrays.equals(tsdir.getHash(), Utils.getHash(value))) {
                    
                    logger.info("Value successfully retrieved from provider {}", replica);
                    return value;
                    
                } else      // the hash doesn't match: byzantine fault: let's try with the other ones
                    continue;
                
            } else {        // this could be due to a) byzantine replicas or b) concurrent gc
                
                byte[] newRawMetadataValue = mds.tsRead(key);
                if (newRawMetadataValue != null) {
                    
                    Metadata newtsdir = new Metadata(newRawMetadataValue);
                    if (newtsdir.getTs().isGreater(tsdir.getTs())) {    // it's because of concurrent gc
                        logger.warn("Could not get the value of {} on replica {} because of concurrent gc. Restarting read.", 
                                    key, replica);
                        return read(key);                               // triggers recursive read
                    } else
                        continue;                                       // otherwise it's because of a byzantine fault: let's try with the other ones
                    
                } else {                                                // the value does not exist anymore [XXX why would it happen?]
                    logger.warn("Hybris could not find the metadata associated with key {}.", key);
                    return null;
                }
            }
        }
        
        logger.warn("Hybris could not manage to retrieve the data associated with key {} from cloud stores.", key);
        return null;
    }

    
    public void write(String key, byte[] value) throws HybrisException {
        
        Timestamp ts;
        byte[] rawMetadataValue = mds.tsRead(key);
        if (rawMetadataValue == null) 
            ts = new Timestamp(0, Utils.getClientId());
        else 
            ts = new Metadata(rawMetadataValue).getTs();
        ts.inc( Utils.getClientId() );
        
        List<String> savedReplicasRef = kvs.put(Utils.getKvsKey(key, ts), value);
        if (savedReplicasRef == null) {
            logger.warn("Hybris could not manage to store data in cloud stores for key {}.", key);
            throw new HybrisException("Hybris could not manage to store data in cloud stores");
        }
        StringBuilder strBld = new StringBuilder("Data successfully stored to these replicas: ");
        for (String cloud : savedReplicasRef) strBld.append(cloud + " ");
        logger.info(strBld.toString());
        
        try {
            mds.tsWrite(key, new Metadata(ts, Utils.getHash(value), savedReplicasRef));
        } catch (HybrisException e) {
            logger.warn("Hybris could not manage to store metadata on Zookeeper for key {}.", key);
            kvsGc(Utils.getKvsKey(key, ts), savedReplicasRef);  // clean up un-referenced data in the clouds
            throw new HybrisException("Hybris could not manage to store the metadata on Zookeeper");
        }
    }
    
    public void gc(String key) throws HybrisException {
        
        byte[] rawMetadataValue = mds.tsRead(key);
        if (rawMetadataValue == null) {
            logger.debug("Hybris could not find the metadata associated with key {}.", key);
            return;
        }
        
        Metadata tsdir = new Metadata(rawMetadataValue);
        for (String cloud : tsdir.getReplicasLst())
            try {
                kvs.deleteKeyFromCloud(cloud, Utils.getKvsKey(key, tsdir.getTs())); // XXX and all the previous versions..?
            } catch (IOException e) {
                logger.warn("error while deleting {} from {}.", key, cloud);
            }
        
        mds.delete(key);
    }
    
    
    // =======================================================================================
    //                                 PRIVATE METHODS
    // ---------------------------------------------------------------------------------------
    
    private void kvsGc(String kvsKey, List<String> replicas) {
        
        for (String cloud : replicas)
            try {
                kvs.deleteKeyFromCloud(cloud, kvsKey);
            } catch (IOException e) {
                logger.warn("error while deleting {} from {}.", kvsKey, cloud);
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
