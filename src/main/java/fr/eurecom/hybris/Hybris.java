package fr.eurecom.hybris;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.kvs.CloudProvider;
import fr.eurecom.hybris.kvs.KvStore;
import fr.eurecom.hybris.kvs.KvStore.KvsPutWorker;
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
    
    public Hybris(String zkAddress, String zkRoot, 
                    String kvsRoot, boolean kvsTestOnStartup, int t) throws HybrisException {
        
        try {
            mds = new MdStore(zkAddress, zkRoot);
        } catch (IOException e) {
            logger.error("Could not initialize the Zookeeper metadata store.", e);
            throw new HybrisException("Could not initialize the Zookeeper metadata store.");
        }
        
        kvs = new KvStore(zkRoot, kvsTestOnStartup);
        this.quorum = t + 1;
    }
    
   
    /* ---------------------------------------------------------------------------------------
                                            Public APIs
       --------------------------------------------------------------------------------------- */


    public void writeParallel(String key, byte[] value) throws HybrisException {
        
        Timestamp ts;
        Stat stat = new Stat();
        stat.setVersion(MdStore.NONODE);    // if it stays unchanged after the read, the znode does not exist
        Metadata md = mds.tsRead(key, stat);
        if (md == null)
            ts = new Timestamp(0, Utils.getClientId());
        else {
            ts = md.getTs();
            ts.inc( Utils.getClientId() );
        }
        
        List<CloudProvider> savedReplicasLst = new ArrayList<CloudProvider>();
        String kvsKey = Utils.getKvsKey(key, ts);
        ExecutorService executor = Executors.newFixedThreadPool(this.quorum);
        List<Future<CloudProvider>> futureLst = new ArrayList<Future<CloudProvider>>(this.quorum);
        int idxFrom = 0; int idxTo = this.quorum;
        do {
            synchronized(kvs.getProviders()) {
                for (CloudProvider provider : kvs.getProviders().subList(idxFrom, idxTo))
                    futureLst.add(executor.submit(new KvsPutWorker(provider, kvsKey, value)));
            }
            
            CloudProvider savedReplica = null;
            for (Future<CloudProvider> future : futureLst)
                try {
                    savedReplica = future.get();
                    if (savedReplica != null) {
                        logger.debug("Data stored in {}", savedReplica.getId());
                        savedReplicasLst.add(savedReplica);
                    }
                    if (savedReplicasLst.size() >= this.quorum)
                        break;
                } catch (InterruptedException | ExecutionException e1) {
                    logger.warn("Exception on the parallel task execution", e1);
                }
            
            idxFrom = idxTo;
            idxTo = kvs.getProviders().size() > idxTo + this.quorum ? 
                        idxTo + this.quorum : kvs.getProviders().size();
        
        } while (savedReplicasLst.size() < this.quorum && idxTo < kvs.getProviders().size());
        executor.shutdown();
        
        if (savedReplicasLst.size() < this.quorum) {
            kvsGc(kvsKey, savedReplicasLst);                    // TODO make asynchronous
            logger.warn("Hybris could not manage to store data in cloud stores for key {}.", key);
            throw new HybrisException("Hybris could not manage to store data in cloud stores");
        } 
               
        try {
            mds.tsWrite(key, new Metadata(ts, Utils.getHash(value), savedReplicasLst), stat.getVersion());
        } catch (HybrisException e) {
            kvsGc(kvsKey, savedReplicasLst);                    // TODO make asynchronous
            logger.warn("Hybris could not manage to store metadata on Zookeeper for key {}.", key);
            throw new HybrisException("Hybris could not manage to store the metadata on Zookeeper");
        }
        
        StringBuilder strBld = new StringBuilder("Data successfully stored to these replicas: ");
        for (CloudProvider cloud : savedReplicasLst) strBld.append(cloud.getId() + " ");
        logger.info(strBld.toString());
    }
    
    
    public void write(String key, byte[] value) throws HybrisException {
        
        Timestamp ts;
        Stat stat = new Stat();
        stat.setVersion(MdStore.NONODE);    // if it stays unchanged after the read, the znode does not exist
        Metadata md = mds.tsRead(key, stat);
        if (md == null)
            ts = new Timestamp(0, Utils.getClientId());
        else {
            ts = md.getTs();
            ts.inc( Utils.getClientId() );
        }
        
        List<CloudProvider> savedReplicasLst = new ArrayList<CloudProvider>();
        String kvsKey = Utils.getKvsKey(key, ts);
        
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders())
                try {                                           // TODO parallelize
                    logger.debug("Storing {} on {}...", key, provider.getId());
                    kvs.put(provider, kvsKey, value);
                    logger.debug("Finished storing {} on {}.", key, provider.getId());
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
            mds.tsWrite(key, new Metadata(ts, Utils.getHash(value), savedReplicasLst), stat.getVersion());
        } catch (HybrisException e) {
            kvsGc(kvsKey, savedReplicasLst);                    // TODO make asynchronous
            logger.warn("Hybris could not manage to store metadata on Zookeeper for key {}.", key);
            throw new HybrisException("Hybris could not manage to store the metadata on Zookeeper");
        }
        
        StringBuilder strBld = new StringBuilder("Data successfully stored to these replicas: ");
        for (CloudProvider cloud : savedReplicasLst) strBld.append(cloud.getId() + " ");
        logger.info(strBld.toString());
    }
    
    
    public void write(String key, byte[] value, int version) throws HybrisException {
        // TODO conditional write
    }
    

    public byte[] read(String key) throws HybrisException {
        
        Metadata md = mds.tsRead(key, null);
        if (md == null) {
            logger.warn("Hybris could not find metadata associated with key {}.", key);
            return null;
        }
        
        byte[] value = null;
        String kvsKey = Utils.getKvsKey(key, md.getTs());
        
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders()) {
                
                if (!md.getReplicasLst().contains(provider)) 
                    continue;
                
                try {
                    value = kvs.get(provider, kvsKey);
                } catch (IOException e) {
                    logger.warn("Error while trying to retrieve " + key + " from " + provider.getId(), e);
                    continue;
                }
                
                if (value != null) {
                    if (Arrays.equals(md.getHash(), Utils.getHash(value))) {
                        logger.info("Value successfully retrieved from provider {}", provider.getId());
                        return value;
                    } else      // the hash doesn't match: byzantine fault: let's try with the other ones
                        continue;
                } else {        // this could be due to:
                                // a. byzantine replicas 
                                // b. concurrent gc 
                    Metadata newMd = mds.tsRead(key, null);
                    if (newMd != null) {
                        if (newMd.getTs().isGreater(md.getTs())) {    // it's because of concurrent gc
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

    
    public void delete(String key) throws HybrisException {
        
        Metadata md = mds.tsRead(key, null);
        if (md == null) {
            logger.debug("Hybris could not find the metadata associated with key {}.", key);
            return;
        }
        
        String kvsKey = Utils.getKvsKey(key, md.getTs());
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders()) {
                
                if (!md.getReplicasLst().contains(provider)) 
                    continue;
                
                try {
                    kvs.delete(provider, kvsKey);
                } catch (IOException e) {
                    logger.warn("error while deleting {} from {}.", key, provider.getId());
                }
            }
        }
        
        mds.delete(key);
    }
    
    
    public List<String> list() throws HybrisException {
        return mds.list();
    }

    
    public Map<String, Metadata> getAllMetadata() throws HybrisException {
        return mds.getAll();
    }
    
    
    /**
     * Delete all the keys on KVS which are not present on MDS or obsolete or malformed.
     * @throws HybrisException
     */
    public void batchGc() throws HybrisException {
        
        Map<String, Metadata> mdMap = mds.getAll();     // !! heavy operation
        
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders()) {
                
                List<String> kvsKeys;
                try {
                    kvsKeys = kvs.list(provider);
                } catch (IOException e) {
                    logger.warn("GC: Could not list {} container", provider.getId());
                    continue;
                }
                
                for (String kvsKey : kvsKeys) {
                    
                    String key = ""; Timestamp kvTs = null; 
                    boolean malformedKey = false;
                    try {
                        key = Utils.getKeyFromKvsKey(kvsKey);
                        kvTs = Utils.getTimestampfromKvsKey(kvsKey);
                    } catch(IndexOutOfBoundsException e) {
                        malformedKey = true;
                    }
                    
                    if ( (malformedKey) || (!mdMap.keySet().contains(key)) ||
                           (mdMap.get(key).getTs().isGreater(kvTs))) {
                        try {
                            kvs.delete(provider, kvsKey);
                        } catch (IOException e) {
                            logger.warn("GC: Could not delete {} from {}", kvsKey, provider.getId());
                            continue;
                        }
                        logger.debug("GC: deleted {} from {}", kvsKey, provider.getId());
                    }
                }
            }
        }
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
        hybris.batchGc();
//        hybris.write("mykey", "my_value".getBytes());
//        String value = new String(hybris.read("mykey"));
//        System.out.println("Read output: " + value);
    }
}