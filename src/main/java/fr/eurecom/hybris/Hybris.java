package fr.eurecom.hybris;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.kvs.KvStore;
import fr.eurecom.hybris.mdstore.MdStore;
import fr.eurecom.hybris.mdstore.TsDir;

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
            throw new HybrisException("Hybris could not find the metadata associated with key " + key);
        }
        
        TsDir tsdir = new TsDir(rawMetadataValue);
        byte[] value = null; 
        for (String provider : tsdir.getReplicasLst()){
            
            value = kvs.getFromCloud(provider, key);
            if ((value != null) && 
                    (Arrays.equals(tsdir.getHash(), Utils.getHash(value)))) {
                logger.info("Value successfully retrieved from provider {}", provider);
                return value;
            }
        }
        
        logger.warn("Hybris could not manage to retrieve the data from cloud stores.");
        throw new HybrisException("Hybris could not manage to retrieve the data from cloud stores.");
    }

    public void write(String key, byte[] value) throws HybrisException {
        
        long ts = System.currentTimeMillis();
        
        List<String> savedReplicasRef = kvs.put(key, value);
        if (savedReplicasRef == null) {
            logger.warn("Hybris could not manage to store data in cloud stores for key {}.", key);
            throw new HybrisException("Hybris could not manage to store data in cloud stores");
        }
        logger.debug("Data successfully saved on these clouds:");
        for (String cloud : savedReplicasRef)
            logger.debug("\t * {}", cloud);
        
        try {
            mds.tsWrite(key, new TsDir(ts, Utils.getHash(value), savedReplicasRef));
        } catch (IOException e) {
            logger.warn("Hybris could not manage to store metadata on Zookeeper for key {}.", key);
            kvsGc(key, savedReplicasRef);  // clean up un-referenced data in the clouds
            throw new HybrisException("Hybris could not manage to store the metadata on Zookeeper");
        }
    }
    
    public void gc(String key) {
        
        byte[] rawMetadataValue = mds.tsRead(key);
        if (rawMetadataValue == null) {
            logger.debug("Hybris could not find the metadata associated with key {}.", key);
            return;
        }
        
        TsDir tsdir = new TsDir(rawMetadataValue);
        for (String cloud : tsdir.getReplicasLst())
            try {
                kvs.deleteKeyFromCloud(cloud, key);
            } catch (IOException e) {
                logger.warn("error while deleting {} from {}.", key, cloud);
            }
    }
    
    
    // =======================================================================================
    //                                 PRIVATE METHODS
    // ---------------------------------------------------------------------------------------
    
    private void kvsGc(String key, List<String> replicas) {
        
        for (String cloud : replicas)
            try {
                kvs.deleteKeyFromCloud(cloud, key);
            } catch (IOException e) {
                logger.error("error while deleting {} from {}.", key, cloud);
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
