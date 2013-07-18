package fr.eurecom.hybris;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.kvs.CloudProvider;
import fr.eurecom.hybris.kvs.KvStore;
import fr.eurecom.hybris.mds.MdStore;
import fr.eurecom.hybris.mds.Metadata;
import fr.eurecom.hybris.mds.Metadata.Timestamp;


/**
 * Hybris cloud storage library main class. 
 * @author p.viotti
 */
public class Hybris {

    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);
    
    private MdStore mds;
    private KvStore kvs;
    
    private int quorum;
    
    /* read and write timeouts for cloud communications */
    private int TIMEOUT_WRITE;
    private int TIMEOUT_READ;
    
    private boolean gcEnabled;
    
    public Hybris() throws HybrisException {
        
        Config conf = Config.getInstance();
        try {
            conf.loadProperties();
            mds = new MdStore(conf.getProperty(Config.MDS_ADDR), 
                                conf.getProperty(Config.MDS_ROOT));
            kvs = new KvStore(conf.getProperty(Config.KVS_ACCOUNTSFILE),
                                conf.getProperty(Config.KVS_ROOT), 
                                Boolean.parseBoolean(conf.getProperty(Config.KVS_TESTSONSTARTUP)));
        } catch (IOException e) {
            logger.error("Could not initialize Zookeeper or the cloud storage accounts.", e);
            throw new HybrisException("Could not initialize Zookeeper or the cloud storage accounts.", e);
        }
        
        int t = Integer.parseInt(conf.getProperty(Config.HS_T));
        this.quorum = t + 1;
        this.TIMEOUT_WRITE = Integer.parseInt(conf.getProperty(Config.HS_TO_WRITE));
        this.TIMEOUT_READ = Integer.parseInt(conf.getProperty(Config.HS_TO_READ));
        this.gcEnabled = Boolean.parseBoolean(conf.getProperty(Config.HS_GC));
    }
    
    public Hybris(String zkAddress, String zkRoot, 
                    String kvsAccountFile, String kvsRoot, boolean kvsTestOnStartup, 
                    int t, int writeTimeout, int readTimeout, boolean gcEnabled) throws HybrisException {
        try {
            mds = new MdStore(zkAddress, zkRoot);
            kvs = new KvStore(kvsAccountFile, kvsRoot, kvsTestOnStartup);
        } catch (IOException e) {
            logger.error("Could not initialize Zookeeper or the cloud storage accounts.", e);
            throw new HybrisException("Could not initialize Zookeeper or the cloud storage accounts", e);
        }
        
        this.quorum = t + 1;
        this.TIMEOUT_WRITE = writeTimeout;
        this.TIMEOUT_READ = readTimeout;
        this.gcEnabled = gcEnabled;
    }
    
   
    /* ---------------------------------------------------------------------------------------
                                            Public APIs
       --------------------------------------------------------------------------------------- */

    /**
     * Write function: write a byte array associated with a key.
     * @param key
     * @param value
     * @throws HybrisException
     */
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
        ExecutorService executor = Executors.newFixedThreadPool(this.quorum);
        List<Future<CloudProvider>> futureLst = new ArrayList<Future<CloudProvider>>(this.quorum);
        int idxFrom = 0; int idxTo = this.quorum;
        do {
            synchronized(kvs.getProviders()) {
                for (CloudProvider provider : kvs.getProviders().subList(idxFrom, idxTo))
                    futureLst.add(executor.submit(kvs.new KvsPutWorker(provider, kvsKey, value)));
            }
            
            CloudProvider savedReplica = null;
            for (Future<CloudProvider> future : futureLst)
                try {
                    savedReplica = future.get(TIMEOUT_WRITE, TimeUnit.SECONDS);
                    if (savedReplica != null) {
                        logger.debug("Data stored in {}", savedReplica.getId());
                        savedReplicasLst.add(savedReplica);
                    }
                    if (savedReplicasLst.size() >= this.quorum)
                        break;
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    logger.warn("Exception on the parallel task execution", e);
                }
            
            idxFrom = idxTo;
            idxTo = kvs.getProviders().size() > idxTo + this.quorum ? 
                        idxTo + this.quorum : kvs.getProviders().size();
        
        } while (savedReplicasLst.size() < this.quorum && idxFrom < idxTo);
        executor.shutdown();
        
        if (savedReplicasLst.size() < this.quorum) {
            if (gcEnabled) (mds.new GcMarker(key, ts, savedReplicasLst)).start();
            logger.warn("Hybris could not manage to store data in cloud stores for key {}.", key);
            throw new HybrisException("Hybris could not manage to store data in cloud stores");
        }
        
        boolean overwritten = false;
        try {
            overwritten = mds.tsWrite(key, new Metadata(ts, Utils.getHash(value), savedReplicasLst), stat.getVersion());
        } catch (HybrisException e) {
            if (gcEnabled) (mds.new GcMarker(key, ts, savedReplicasLst)).start();
            logger.warn("Hybris could not manage to store metadata on Zookeeper for key {}.", key);
            throw new HybrisException("Hybris could not manage to store the metadata on Zookeeper");
        }
        
        if (gcEnabled && overwritten) (mds.new GcMarker(key)).start();
        
        logger.info("Data successfully stored on: {}", savedReplicasLst);
    }
    
    /**
     * Write function: write a byte array associated with a key - serial upload version.
     * @deprecated : use the parallel version {@link fr.eurecom.hybris.Hybris.write}
     * @param key
     * @param value
     * @throws HybrisException
     */
    public void writeSerial(String key, byte[] value) throws HybrisException {
        
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
                try {                                           // Notice: serial put
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
            if (gcEnabled) (mds.new GcMarker(key, ts, savedReplicasLst)).start();
            logger.warn("Hybris could not manage to store data in cloud stores for key {}.", key);
            throw new HybrisException("Hybris could not manage to store data in cloud stores");
        } 
        
        boolean modified = false;
        try {
            modified = mds.tsWrite(key, new Metadata(ts, Utils.getHash(value), savedReplicasLst), stat.getVersion());
        } catch (HybrisException e) {
            if (gcEnabled) (mds.new GcMarker(key, ts, savedReplicasLst)).start();
            logger.warn("Hybris could not manage to store metadata on Zookeeper for key {}.", key);
            throw new HybrisException("Hybris could not manage to store the metadata on Zookeeper");
        }
        
        if (gcEnabled && modified)
            (mds.new GcMarker(key)).start();
        
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
                } else {        /* this could be due to:
                                 * a. byzantine replicas 
                                 * b. concurrent gc
                                 */ 
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
    
    
    /* -------------------------------------- GC functions -------------------------------------- */
    
    /**
     * gc(): delete all the orphan or stale keys from KVS which are indexed on MDS.
     * @throws HybrisException
     */
    public void gc() throws HybrisException {
        
        // Orphans
        Map<String, Metadata> orphans = mds.getOrphans();
        Set<String> orphanKeys = orphans.keySet();
        for (Iterator<String> it = orphanKeys.iterator(); it.hasNext();) {
            String kvsKey = it.next();
            Metadata md = orphans.get(kvsKey);
            boolean error = false;
            
            synchronized(kvs.getProviders()) {
                for (CloudProvider provider : kvs.getProviders()) {
                    
                    if (!md.getReplicasLst().contains(provider)) 
                        continue;
                    
                    try {
                        kvs.delete(provider, kvsKey);
                    } catch (IOException e) {
                        error = true;
                        logger.warn("error while deleting {} from {}.", kvsKey, provider.getId());
                    }
                }
            }
            
            if (error) it.remove();
        }
        mds.removeOrphanKeys(orphanKeys);
        
        // Stale
        List<String> staleKeys = mds.getStaleKeys();
        for (String key : staleKeys) {
            try {
                gc(key);
            } catch (HybrisException e) {
                logger.warn("GC: could not gc key {}", key);
            }
        }
    }
    
    
    /**
     * gc(key): delete from KVS stale data associated with <key>.
     * @param key
     * @throws HybrisException
     */
    public void gc(String key) throws HybrisException {
        
        Metadata md = mds.tsRead(key, null);
        if (md == null) {
            logger.debug("GC: Hybris could not find the metadata associated with key {}.", key);
            return;
        }
        
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
                    String prefixKey = ""; Timestamp kvTs = null; 
                    boolean malformedKey = false;
                    try {
                        prefixKey = Utils.getKeyFromKvsKey(kvsKey);
                        kvTs = Utils.getTimestampfromKvsKey(kvsKey);
                    } catch(IndexOutOfBoundsException e) {
                        malformedKey = true;
                    }
                    
                    if ( malformedKey || 
                            ((key.equals(prefixKey)) && (md.getTs().isGreater(kvTs))) )  {
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
        
        mds.removeStaleKey(key);
    }
    
    
    /**
     * batchGc(key): delete all the keys from KVS which are not present on MDS or obsolete or malformed.
     * This function does a complete MDS dump and a complete KVS listing, 
     * so beware that it can be very heavy and slow.
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
                           (mdMap.get(key).getTs().isGreater(kvTs)) ) {
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
        //mds.emptyStaleAndOrphansContainers();
    }

    
    /**
     * TODO TEMP for debugging purposes
     * @throws IOException 
     * @throws HybrisException 
     */
//    public static void main(String[] args) throws HybrisException {
//        Hybris hybris = new Hybris();
//        hybris.write("mykey", "my_value".getBytes());
//        String value = new String(hybris.read("mykey"));
//        System.out.println("Read output: " + value);
//    }
}