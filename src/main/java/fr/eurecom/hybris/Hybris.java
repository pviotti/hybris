package fr.eurecom.hybris;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.kvs.KvsManager;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.mds.MdsManager;
import fr.eurecom.hybris.mds.Metadata;
import fr.eurecom.hybris.mds.Metadata.Timestamp;


/**
 * Hybris cloud storage library main class.
 * @author P. Viotti
 */
public class Hybris {

    private static final Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private MdsManager mds;
    private KvsManager kvs;

    /* caching */
    private MemcachedClient cache;
    private int cacheExp;   // default cache expiration timeout [s]
    private boolean cacheEnabled;
    private enum CachePolicy { ONREAD, ONWRITE };
    private CachePolicy cachePolicy;

    private final int quorum;

    /* read and write timeouts for cloud communications [s] */
    private final int TIMEOUT_WRITE;
    private final int TIMEOUT_READ;     // TODO

    /* GC */
    private final boolean gcEnabled;

    /* confidentiality */
    private final boolean cryptoEnabled;

    private final String clientId;

    private HashMap<String, HybrisWatcher> notifications;


    /**
     * Creates a Hybris client with the parameters specified
     * in the given configuration file.
     * @param path to the Hybris properties file
     * @throws HybrisException
     */
    public Hybris(String propertiesFile) throws HybrisException {

        Config conf = Config.getInstance();
        try {
            conf.loadProperties(propertiesFile);
            this.mds = new MdsManager(conf.getProperty(Config.MDS_ADDR),
                    conf.getProperty(Config.MDS_ROOT));
            this.kvs = new KvsManager(conf.getProperty(Config.KVS_ACCOUNTSFILE),
                    conf.getProperty(Config.KVS_ROOT),
                    Boolean.parseBoolean(conf.getProperty(Config.KVS_TESTSONSTARTUP)));
        } catch (IOException e) {
            logger.error("Could not initialize Zookeeper or the cloud storage KvStores.", e);
            throw new HybrisException("Could not initialize Zookeeper or the cloud storage KvStores.", e);
        }

        this.cacheEnabled = Boolean.parseBoolean(conf.getProperty(Config.CACHE_ENABLED));
        if (this.cacheEnabled)
            try {
                Properties sysProp = System.getProperties();
                sysProp.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
                System.setProperties(sysProp);
                this.cache = new MemcachedClient(new BinaryConnectionFactory(),
                        AddrUtil.getAddresses(conf.getProperty(Config.CACHE_ADDRESS)));
                this.cacheExp = Integer.parseInt(conf.getProperty(Config.CACHE_EXP));
                this.cachePolicy = CachePolicy.valueOf(conf.getProperty(Config.CACHE_POLICY).toUpperCase());
            } catch (Exception e) {
                logger.warn("Could not initialize the caching client. Please check its settings.", e);
                this.cacheEnabled = false;
            }

        int t = Integer.parseInt(conf.getProperty(Config.HS_F));
        this.quorum = t + 1;
        this.TIMEOUT_WRITE = Integer.parseInt(conf.getProperty(Config.HS_TO_WRITE));
        this.TIMEOUT_READ = Integer.parseInt(conf.getProperty(Config.HS_TO_READ));
        this.gcEnabled = Boolean.parseBoolean(conf.getProperty(Config.HS_GC));
        this.cryptoEnabled = Boolean.parseBoolean(conf.getProperty(Config.HS_CRYPTO));
        String cid = conf.getProperty(Config.HS_CLIENTID);
        if (cid != null)    this.clientId = cid;
        else                this.clientId = Utils.generateClientId();
    }

    /**
     * Creates a Hybris client.
     * @param zkAddress - list of comma separated addresses of Zookeeper cluster servers.
     * @param zkRoot - Zookeeper znode to adopt as root by the MdsManager. If not existing it will be created.
     * @param kvsAccountFile - path of the property file containing KVS accounts details.
     * @param kvsRoot - KVS container to adopt as root by the KVSs. If not existing it will be created.
     * @param kvsTestOnStartup - perform latency tests and sort KVSs accordingly.
     * @param clientId - clientId - if null, it will be randomly generated.
     * @param t - number of tolerated faulty KVS replicas.
     * @param writeTimeout - timeout to adopt when writing on KVSs (seconds).
     * @param readTimeout - timeout to adopt when reading from KVSs (seconds).
     * @param gcEnabled - enables KVS garbage collection.
     * @param cryptoEnabled - enables data confidentiality support.
     * @param cachingEnable - enables caching.
     * @param memcachedAddrs - list of comma separated addresses of Memcached servers.
     * @param cacheExp - caching default expiration timeout (seconds).
     * @param cachePolicy - caching policy, either "onread" or "onwrite"
     * @throws HybrisException
     */
    public Hybris(String zkAddress, String zkRoot,
            String kvsAccountFile, String kvsRoot, boolean kvsTestOnStartup,
            String clientId, int t, int writeTimeout, int readTimeout, boolean gcEnabled, boolean cryptoEnabled,
            boolean cachingEnable, String memcachedAddrs, int cacheExp, String cachePolicy) throws HybrisException {
        try {
            this.mds = new MdsManager(zkAddress, zkRoot);
            this.kvs = new KvsManager(kvsAccountFile, kvsRoot, kvsTestOnStartup);
        } catch (IOException e) {
            logger.error("Could not initialize Zookeeper or the cloud storage KvStores.", e);
            throw new HybrisException("Could not initialize Zookeeper or the cloud storage KvStores", e);
        }

        this.cacheEnabled = cachingEnable;
        if (this.cacheEnabled)
            try {
                Properties sysProp = System.getProperties();
                sysProp.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
                System.setProperties(sysProp);
                this.cache = new MemcachedClient(new BinaryConnectionFactory(),
                        AddrUtil.getAddresses(memcachedAddrs));
                this.cacheExp = cacheExp;
                this.cachePolicy = CachePolicy.valueOf(cachePolicy.toUpperCase());
            } catch (Exception e) {
                logger.warn("Could not initialize the caching client. Please check its settings.", e);
                this.cacheEnabled = false;
            }

        this.quorum = t + 1;
        this.TIMEOUT_WRITE = writeTimeout;
        this.TIMEOUT_READ = readTimeout;
        this.gcEnabled = gcEnabled;
        this.cryptoEnabled = cryptoEnabled;
        if (clientId != null)   this.clientId = clientId;
        else                    this.clientId = Utils.generateClientId();
    }


    /* ---------------------------------------------------------------------------------------
                                            Public APIs
       --------------------------------------------------------------------------------------- */

    /**
     * Writes a byte array associated with a key.
     * @param key
     * @param value
     * @return the list of Kvs in which Hybris stored the data
     * @throws HybrisException
     */
    public List<Kvs> put(String key, byte[] value) throws HybrisException {

        Timestamp ts;
        Stat stat = new Stat();
        Metadata md = this.mds.tsRead(key, stat);
        if (md == null) {
            ts = new Timestamp(0, this.clientId);
            stat.setVersion(MdsManager.NONODE);
        } else {
            ts = md.getTs();
            ts.inc( this.clientId );
        }

        byte[] cryptoKeyIV = null;
        if (this.cryptoEnabled) {
            if (md == null || md.getCryptoKeyIV() == null) {
                logger.debug("Generating new encryption key for key {}", key);
                cryptoKeyIV = new byte[Utils.CRYPTO_LENGTH];
                cryptoKeyIV = Utils.generateRandomBytes(cryptoKeyIV);
            } else
                cryptoKeyIV = md.getCryptoKeyIV();

            try {
                logger.debug("Encrypting data for key {}", key);
                value = Utils.encrypt(value, cryptoKeyIV);
            } catch(GeneralSecurityException e) {
                logger.error("Could not encrypt data", e);
                cryptoKeyIV = null;
            }
        }

        List<Kvs> savedReplicasLst = new ArrayList<Kvs>();
        String kvsKey = Utils.getKvsKey(key, ts);
        ExecutorService executor = Executors.newFixedThreadPool(this.quorum);
        CompletionService<Kvs> compServ = new ExecutorCompletionService<Kvs>(executor);
        int idxFrom = 0; int idxTo = this.quorum; long start; Future<Kvs> future;
        do {
            List<Kvs> kvsSublst = this.kvs.getKvsSortedByWriteLatency().subList(idxFrom, idxTo);
            start = System.currentTimeMillis();
            for (Kvs kvStore : kvsSublst)
                compServ.submit(this.kvs.new KvsPutWorker(kvStore, kvsKey, value));

            Kvs savedReplica = null;
            for (int i=0; i<kvsSublst.size(); i++)
                try {
                    future =  compServ.poll(this.TIMEOUT_WRITE, TimeUnit.SECONDS);
                    if (future != null && (savedReplica = future.get()) != null) {
                        logger.debug("Data stored on {}, {} ms", savedReplica,
                                System.currentTimeMillis() - start);
                        savedReplicasLst.add(savedReplica);
                        if (savedReplicasLst.size() >= this.quorum)
                            break;
                    }
                } catch (InterruptedException | ExecutionException e) {
                    logger.warn("Exception on write task execution", e);
                }

            idxFrom = idxTo;
            idxTo = this.kvs.getKvsList().size() > idxTo + this.quorum ?
                    idxTo + this.quorum : this.kvs.getKvsList().size();

        } while (savedReplicasLst.size() < this.quorum && idxFrom < idxTo);
        executor.shutdown();

        if (savedReplicasLst.size() < this.quorum) {
            if (this.gcEnabled) this.mds.new GcMarker(key, ts, savedReplicasLst).start();
            logger.warn("Could not store data in cloud stores for key {}.", key);
            throw new HybrisException("Could not store data in cloud stores");
        }

        if (this.cacheEnabled && CachePolicy.ONWRITE.equals(this.cachePolicy))
            this.cache.set(kvsKey, this.cacheExp, value);

        boolean overwritten = false;
        try {
            Metadata newMd = new Metadata(ts, Utils.getHash(value), value.length, savedReplicasLst, cryptoKeyIV);
            overwritten = this.mds.tsWrite(key, newMd, stat.getVersion());
        } catch (HybrisException e) {
            if (this.gcEnabled) this.mds.new GcMarker(key, ts, savedReplicasLst).start();
            logger.warn("Could not store metadata on Zookeeper for key {}.", key);
            throw new HybrisException("Could not store the metadata on Zookeeper");
        }

        if (this.gcEnabled && overwritten) this.mds.new GcMarker(key).start();

        logger.info("Data stored on: {}", savedReplicasLst);
        return savedReplicasLst;
    }


    /**
     * Fetches the value associated with <key>.
     * @param key
     * @return a byte array containing the value associated with <key>.
     * @throws HybrisException
     */
    public byte[] get(String key) throws HybrisException {

        Metadata md;
        if (this.notifications.containsKey(key)) {
            // if it's a new attempt after a recursive call, set a watcher
            HybrisWatcher hwatcher = this.new HybrisWatcher();
            this.notifications.put(key, hwatcher);
            md = this.mds.tsRead(key, null, hwatcher);
        } else
            md = this.mds.tsRead(key, null);

        if (md == null || md.isTombstone()) {
            this.notifications.remove(key);
            logger.warn("Could not find metadata associated with key {}.", key);
            return null;
        }

        byte[] value = null;
        String kvsKey = Utils.getKvsKey(key, md.getTs());

        if (this.cacheEnabled) {        // check the cache
            value = (byte[]) this.cache.get(kvsKey);
            if (value != null && Arrays.equals(md.getHash(), Utils.getHash(value))) {

                if (md.getCryptoKeyIV() != null)
                    try {
                        logger.debug("Decrypting data for key {}", key);
                        value = Utils.decrypt(value, md.getCryptoKeyIV());
                    } catch (GeneralSecurityException | UnsupportedEncodingException e) {
                        this.notifications.remove(key);
                        logger.error("Could not decrypt data", e);
                        throw new HybrisException("Could not decrypt data", e);
                    }

                this.notifications.remove(key);
                logger.debug("Value of {} retrieved from cache", key);
                return value;
            }
        }

        for (Kvs kvStore : this.kvs.getKvsSortedByReadLatency()) {

            if (!md.getReplicasLst().contains(kvStore))
                continue;

            try {
                value = this.kvs.get(kvStore, kvsKey);      // TODO check filesize to prevent DOS
            } catch (IOException e) {
                continue;
            }

            if (value != null) {
                if (Arrays.equals(md.getHash(), Utils.getHash(value))) {
                    logger.info("Value of {} retrieved from kvStore {}", key, kvStore);
                    if (this.cacheEnabled && CachePolicy.ONREAD.equals(this.cachePolicy))
                        this.cache.set(kvsKey, this.cacheExp, value);

                    if (md.getCryptoKeyIV() != null)
                        try {
                            logger.debug("Decrypting data for key {}", key);
                            value = Utils.decrypt(value, md.getCryptoKeyIV());
                        } catch (GeneralSecurityException | UnsupportedEncodingException e) {
                            logger.error("Could not decrypt data", e);
                            throw new HybrisException("Could not decrypt data", e);
                        }

                    this.notifications.remove(key);
                    return value;
                } else      // The hash doesn't match: Byzantine fault: let's try with the other clouds
                    continue;
            } else {
                /* This could be due to:
                 * a. Byzantine replicas
                 * b. concurrent gc
                 */
                HybrisWatcher hwatcher = this.new HybrisWatcher();
                this.notifications.put(key, hwatcher);
                Metadata newMd = this.mds.tsRead(key, null, hwatcher);
                if (newMd != null && !newMd.isTombstone()) {
                    if (newMd.getTs().isGreater(md.getTs())) {      // it's because of concurrent GC and a more recent version has been written
                        logger.warn("Found a more recent version of metadata of {}. Restarting read.", key, kvStore);
                        return this.get(key);                       // trigger recursive read
                    } else
                        continue;                                   // otherwise it's because of BF: let's try with the other ones
                } else {        // the value does not exist anymore because of concurrent GC, and no other versions haven't been written
                    logger.warn("Could not find the metadata associated with key {}.", key);
                    break;
                }
            }
        }

        this.notifications.remove(key);
        logger.warn("Could not retrieve the data associated with key {} from cloud stores.", key);
        return null;
    }


    /**
     * Deletes data and metadata associated with <key>.
     * @param key
     * @throws HybrisException
     */
    public void delete(String key) throws HybrisException {

        Stat stat = new Stat();
        Metadata md = this.mds.tsRead(key, stat);
        if (md == null) {
            logger.debug("Could not find the metadata associated with key {}.", key);
            return;
        }
        Timestamp ts = md.getTs();
        ts.inc( this.clientId );
        Metadata tombstone = Metadata.getTombstone(ts);

        String kvsKey = Utils.getKvsKey(key, md.getTs());
        for (Kvs kvStore : this.kvs.getKvsList()) {

            if (!md.getReplicasLst().contains(kvStore))
                continue;

            try {
                this.kvs.delete(kvStore, kvsKey);
            } catch (IOException e) {
                logger.warn("Could not delete {} from {}", kvsKey, kvStore);
            }
        }
        this.mds.delete(key, tombstone, stat.getVersion());
    }


    /**
     * Lists keys by inquiring the MDS.
     * @return
     * @throws HybrisException
     */
    public List<String> list() throws HybrisException {
        return this.mds.list();
    }


    /**
     * Fetches all metadata currently stored on MDS.
     * XXX not scalable - for debugging purposes
     * @return
     * @throws HybrisException
     */
    public Map<String, Metadata> getAllMetadata() throws HybrisException {
        return this.mds.getAll();
    }


    /**
     * Tests KVSs latencies and sort them accordingly.
     * @param testDataSize [kB]
     */
    public void testLatencyAndSortClouds(int testDataSize) {
        this.kvs.testLatencyAndSortClouds(testDataSize);
    }


    /**
     * Stops Hybris client.
     * Closes connections and shuts off thread pools.
     */
    public void shutdown() {
        for (Kvs kvStore : this.kvs.getKvsList())
            this.kvs.shutdown(kvStore);
        this.mds.shutdown();
        if (this.cacheEnabled)
            this.cache.shutdown();
    }


    /* -------------------------------------- HybrisWatcher -------------------------------------- */

    /**
     * Class in charge of handling ZooKeeper notifications.
     * @author P. Viotti
     */
    public class HybrisWatcher implements CuratorWatcher {

        private boolean changed = false;
        public boolean isChanged() { return this.changed; }

        /**
         * Process a notification sent by ZooKeeper
         * @see org.apache.curator.framework.api.CuratorWatcher#process(org.apache.zookeeper.WatchedEvent)
         */
        public void process(WatchedEvent event) throws Exception {
            this.changed = true;
        }
    }


    /* -------------------------------------- GcManager -------------------------------------- */

    /**
     * Class in charge of performing garbage collection tasks.
     * @author P. Viotti
     */
    public class GcManager {

        /**
         * Deletes from KVSs all orphan or stale keys which are indexed on MDS.
         * @throws HybrisException
         */
        public void gc() throws HybrisException {

            // Orphans
            Map<String, Metadata> orphans = Hybris.this.mds.getOrphans();
            Set<String> orphanKeys = orphans.keySet();
            for (Iterator<String> it = orphanKeys.iterator(); it.hasNext();) {
                String kvsKey = it.next();
                Metadata md = orphans.get(kvsKey);
                boolean error = false;

                for (Kvs kvStore : Hybris.this.kvs.getKvsList()) {

                    if (!md.getReplicasLst().contains(kvStore))
                        continue;

                    try {
                        Hybris.this.kvs.delete(kvStore, kvsKey);
                    } catch (IOException e) {
                        error = true;
                        logger.warn("GC: could not delete {} from {}", kvsKey, kvStore);
                    }
                }

                if (error) it.remove();
            }
            Hybris.this.mds.removeOrphanKeys(orphanKeys);

            // Stale
            List<String> staleKeys = Hybris.this.mds.getStaleKeys();
            for (String key : staleKeys)
                try {
                    this.gc(key);
                } catch (HybrisException e) {
                    logger.warn("GC: could not gc key {}", key);
                }
        }


        /**
         * Deletes from KVSs stale data associated with <key>.
         * @param key
         * @throws HybrisException
         */
        public void gc(String key) throws HybrisException {

            Metadata md = Hybris.this.mds.tsRead(key, null);
            if (md == null) {
                logger.debug("GC: could not find the metadata associated with key {}.", key);
                return;
            }

            for (Kvs kvStore : Hybris.this.kvs.getKvsList()) {

                List<String> kvsKeys;
                try {
                    kvsKeys = Hybris.this.kvs.list(kvStore);
                } catch (IOException e) {
                    logger.warn("GC: could not list {} container", kvStore);
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
                            key.equals(prefixKey) && md.getTs().isGreater(kvTs) )  {
                        try {
                            Hybris.this.kvs.delete(kvStore, kvsKey);
                        } catch (IOException e) {
                            logger.warn("GC: could not delete {} from {}", kvsKey, kvStore);
                            continue;
                        }
                        logger.debug("GC: deleted {} from {}", kvsKey, kvStore);
                    }
                }
            }

            Hybris.this.mds.removeStaleKey(key);
        }


        /**
         * Deletes from KVSs all the keys which are not present on MDS or obsolete or malformed.
         * Heads up: this function does a complete MDS dump and a complete KVS listing,
         * so it can be very slow and resource consuming.
         * @throws HybrisException
         */
        public void batchGc() throws HybrisException {

            Map<String, Metadata> mdMap = Hybris.this.mds.getAll();     // !! heavy operation

            for (Kvs kvStore : Hybris.this.kvs.getKvsList()) {

                List<String> kvsKeys;
                try {
                    kvsKeys = Hybris.this.kvs.list(kvStore);
                } catch (IOException e) {
                    logger.warn("GC: could not list {} container", kvStore);
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

                    if ( malformedKey || !mdMap.keySet().contains(key) ||
                            mdMap.get(key).getTs().isGreater(kvTs) ) {
                        try {
                            Hybris.this.kvs.delete(kvStore, kvsKey);
                        } catch (IOException e) {
                            logger.warn("GC: could not delete {} from {}", kvsKey, kvStore);
                            continue;
                        }
                        logger.debug("GC: deleted {} from {}", kvsKey, kvStore);
                    }
                }
            }
            //mds.emptyStaleAndOrphansContainers();
        }


        /* -------------------------------------- TEMP / DEBUG -------------------------------------- */
        /**
         * XXX TEMP for testing and debugging - cleans up the KVS and MDS containers (!)
         * @throws HybrisException
         */
        public void _emptyContainers() throws HybrisException {
            Hybris.this.mds.emptyMetadataContainer();
            for (Kvs kvStore : Hybris.this.kvs.getKvsList())
                try {
                    Hybris.this.kvs.emptyStorageContainer(kvStore);
                } catch (IOException e) {
                    logger.warn("Could not empty {} container", kvStore);
                }
        }
    }
}
