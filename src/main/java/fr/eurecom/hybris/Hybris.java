/**
 * Copyright (C) 2013 EURECOM (www.eurecom.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.eurecom.hybris;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
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

import fr.eurecom.hybris.EcManager.ChunkState;
import fr.eurecom.hybris.EcManager.EcChunk;
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

    protected MdsManager mds;
    protected KvsManager kvs;
    
    /* erasure coding */
    private EcManager ec;
    private int k;
    private int m;

    /* caching */
    private MemcachedClient cache;
    private int cacheExp;   // default cache expiration timeout [s]
    private boolean cacheEnabled;
    private enum CachePolicy { ONREAD, ONWRITE };
    private CachePolicy cachePolicy;

    private int quorum;

    /* read and write timeouts for cloud communications [s] */
    private int TIMEOUT_WRITE;
    private int TIMEOUT_READ;

    /* GC */
    private GcManager gc;
    private boolean gcEnabled;

    /* confidentiality */
    private boolean cryptoEnabled;
    private byte[] IV;
    
    /* erasure coding */
    private boolean ecEnabled;

    public String clientId;


    /**
     * Creates a Hybris client with the parameters specified
     * in the given configuration file.
     * @param path to the Hybris properties file
     * @throws HybrisException
     */
    public Hybris(String propertiesFile) throws HybrisException {

        Config conf = Config.getInstance();
        try{
            conf.loadProperties(propertiesFile);
        } catch (IOException e) {
            logger.error("Could not read the configuration file " + propertiesFile, e);
            throw new HybrisException("Could not read the configuration file " + propertiesFile, e);
        }
        
        this.configureAndInitialize(conf.getProperty(Config.MDS_ADDR), conf.getProperty(Config.MDS_ROOT),
                conf.getProperty(Config.KVS_ACCOUNTSFILE), conf.getProperty(Config.KVS_ROOT),
                Boolean.parseBoolean(conf.getProperty(Config.KVS_TESTSONSTARTUP)),
                conf.getProperty(Config.HS_CLIENTID), Integer.parseInt(conf.getProperty(Config.HS_F)),
                Integer.parseInt(conf.getProperty(Config.HS_TO_WRITE)),
                Integer.parseInt(conf.getProperty(Config.HS_TO_READ)),
                Boolean.parseBoolean(conf.getProperty(Config.HS_GC)),
                Boolean.parseBoolean(conf.getProperty(Config.HS_CRYPTO)), 
                Boolean.parseBoolean(conf.getProperty(Config.CACHE_ENABLED)), conf.getProperty(Config.CACHE_ADDRESS),
                Integer.parseInt(conf.getProperty(Config.CACHE_EXP)), conf.getProperty(Config.CACHE_POLICY), 
                Boolean.parseBoolean(conf.getProperty(Config.ECODING)), 
                Integer.parseInt(conf.getProperty(Config.ECODING_K)));
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
     * @param ecEnabled - enables erasure coding.
     * @param ecK - erasure coding k parameter (number of data devices).
     * @throws HybrisException
     */
    public Hybris(String zkAddress, String zkRoot,
            String kvsAccountFile, String kvsRoot, boolean kvsTestOnStartup,
            String clientId, int t, int writeTimeout, int readTimeout, boolean gcEnabled, boolean cryptoEnabled,
            boolean cachingEnable, String memcachedAddrs, int cacheExp, String cachePolicy, boolean ecEnabled, int ecK) 
                    throws HybrisException {
        
        this.configureAndInitialize(zkAddress, zkRoot, kvsAccountFile, kvsRoot, kvsTestOnStartup, clientId, 
                t, writeTimeout, readTimeout, gcEnabled, cryptoEnabled, cachingEnable, 
                memcachedAddrs, cacheExp, cachePolicy, ecEnabled, ecK);
    }
    
    private void configureAndInitialize(String zkAddress, String zkRoot,
            String kvsAccountFile, String kvsRoot, boolean kvsTestOnStartup,
            String clientId, int t, int writeTimeout, int readTimeout, boolean gcEnabled, boolean cryptoEnabled,
            boolean cachingEnable, String memcachedAddrs, int cacheExp, String cachePolicy, boolean ecEnabled, int ecK) 
                    throws HybrisException {
        try {
            this.mds = new MdsManager(zkAddress, zkRoot);
            this.kvs = new KvsManager(kvsAccountFile, kvsRoot, kvsTestOnStartup);
        } catch (IOException e) {
            logger.error("Could not initialize ZooKeeper or the cloud storage KvStores.", e);
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
        if (gcEnabled)
            gc = new GcManager(this);
        this.cryptoEnabled = cryptoEnabled;
        if (this.cryptoEnabled)
            this.IV = this.mds.getOrCreateIv();
        
        this.ecEnabled = ecEnabled;
        if (this.ecEnabled) 
            try {
                ec = new EcManager();
                if (ecK <=0) {
                    logger.error("Wrong value for k (<=0), disabling erasure coding.");
                    this.ecEnabled = false;
                } else {
                    this.k = ecK;
                    this.m = t;
                }
            } catch (Exception e) {
                this.ecEnabled = false;
            }

        if (clientId != null)   this.clientId = clientId;
        else                    this.clientId = Utils.generateClientId();
    }


    /* ---------------------------------------------------------------------------------------
                                            Public APIs
       --------------------------------------------------------------------------------------- */
    
    public void setErasureCoding(boolean ecEnabled) {
        this.ecEnabled = ecEnabled;
        int ecK = Integer.parseInt(Config.getInstance().getProperty(Config.ECODING_K));
        if (this.ecEnabled) 
            try {
                ec = new EcManager();
                if (ecK <=0) {
                    logger.error("Wrong value for k (<=0), disabling erasure coding.");
                    this.ecEnabled = false;
                } else {
                    this.k = ecK;
                    this.m = this.quorum -1;
                }
            } catch (Exception e) {
                this.ecEnabled = false;
            }
    }
    
    
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

        byte[] cryptoKey = null;
        if (this.cryptoEnabled) {
            if (md == null || md.getCryptoKey() == null) {
                logger.debug("Generating new encryption key for key {}", key);
                cryptoKey = new byte[Utils.CRYPTO_KEY_LENGTH];
                cryptoKey = Utils.generateRandomBytes(cryptoKey);
            } else
                cryptoKey = md.getCryptoKey();

            try {
                logger.debug("Encrypting data for key {}", key);
                value = Utils.encrypt(value, cryptoKey, this.IV);
            } catch(GeneralSecurityException e) {
                logger.error("Could not encrypt data", e);
                cryptoKey = null;
            }
        }
        
        List<Kvs> savedReplicasLst = new ArrayList<Kvs>();
        String kvsKey = Utils.getKvsKey(key, ts);
        int idxFrom = 0; long start; Future<Kvs> future;
        byte[][] chunkHashes = null;
        
        if (this.ecEnabled) {   // Erasure coding
            
            // TODO storing chunks' hashes in md
            // XXX caching with ec?
            ExecutorService executor = Executors.newFixedThreadPool(m + k);
            CompletionService<Kvs> compServ = new ExecutorCompletionService<Kvs>(executor);
            
            byte[][] encoded = ec.encode(value, k, m);
            EcChunk[] chunks = new EcChunk[encoded.length];
            for (int i=0; i<encoded.length; i++)
                chunks[i] = this.ec.new EcChunk(encoded[i], Utils.getHash(encoded[i]), null, ChunkState.KO);
            int idxTo = k + m;
            boolean completed;
            do {
                completed = true;
                List<Kvs> kvsSublst = this.kvs.getKvsSortedByWriteLatency().subList(idxFrom, idxTo);
                int pending = 0;
                start = System.currentTimeMillis();
                for (int i=0; i<kvsSublst.size(); i++)
                    for (int j=0; j<chunks.length; j++)
                        if (ChunkState.KO.equals(chunks[j].state)) {
                            compServ.submit(this.kvs.new KvsPutWorker(kvsSublst.get(i), kvsKey, chunks[j].data));
                            chunks[j].state = ChunkState.PENDING;
                            chunks[j].kvs = kvsSublst.get(i);
                            pending++;
                            break;
                        }

                Kvs savedReplica = null;
                for (int i=0; i<pending; i++)
                    try {
                        future =  compServ.poll(this.TIMEOUT_WRITE, TimeUnit.SECONDS);
                        if (future != null && 
                                !(savedReplica = future.get()).getId().startsWith(KvsManager.FAIL_PREFIX))

                            for (int j=0; j<chunks.length; j++) {
                                if (ChunkState.PENDING.equals(chunks[j].state) 
                                        && savedReplica.equals(chunks[j].kvs)) {
                                    logger.debug("Chunk {}, {} B, stored on {}, {} ms", i, 
                                            chunks[j].data.length, savedReplica,
                                            System.currentTimeMillis() - start);
                                    chunks[j].state = ChunkState.OK;
                                    break;
                                }
                            }
                        else if (savedReplica != null)
                                for (int j=0; j<chunks.length; j++)
                                    if (savedReplica.getId().contains(chunks[i].kvs.getId()))
                                        chunks[i].state = ChunkState.KO;
                    } catch (InterruptedException | ExecutionException e) {
                        logger.warn("Exception on write task execution", e);
                    }
                
                for (int j=0; j<chunks.length; j++) 
                    if (!ChunkState.OK.equals(chunks[j].state)) 
                        completed = false;
                
                idxFrom = idxTo;
                idxTo = this.kvs.getKvsList().size() > idxTo + k + m?
                        idxTo + k + m: this.kvs.getKvsList().size();

            } while (!completed && idxFrom < idxTo);
            executor.shutdown();
            
            chunkHashes = new byte[chunks.length][];
            for (int j=0; j<chunks.length; j++)
                if (chunks[j].state.equals(ChunkState.OK)) {
                    savedReplicasLst.add(chunks[j].kvs);
                    chunkHashes[j] = chunks[j].hash;
                }
            
            if (!completed) {
                if (this.gcEnabled) this.mds.new GcMarker(key, ts, savedReplicasLst).start();
                logger.warn("Could not store data in cloud stores for key {}.", key);
                throw new HybrisException("Could not store data in cloud stores");
            }
            
        } else {    // Replication
            
            ExecutorService executor = Executors.newFixedThreadPool(this.quorum);
            CompletionService<Kvs> compServ = new ExecutorCompletionService<Kvs>(executor);
            int idxTo = this.quorum;
            do {
                List<Kvs> kvsSublst = this.kvs.getKvsSortedByWriteLatency().subList(idxFrom, idxTo);
                start = System.currentTimeMillis();
                for (Kvs kvStore : kvsSublst)
                    compServ.submit(this.kvs.new KvsPutWorker(kvStore, kvsKey, value));
    
                Kvs savedReplica = null;
                for (int i=0; i<kvsSublst.size(); i++)
                    try {
                        future =  compServ.poll(this.TIMEOUT_WRITE, TimeUnit.SECONDS);
                        if (future != null && 
                                !(savedReplica = future.get()).getId().startsWith(KvsManager.FAIL_PREFIX)) {
                            logger.debug("Data ({} B) stored on {}, {} ms", value.length, savedReplica,
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
        }

        boolean overwritten = false;
        try {
            Metadata newMd;
            if (this.ecEnabled)
                newMd = new Metadata(ts, chunkHashes, savedReplicasLst, value.length, cryptoKey);
            else
                newMd = new Metadata(ts, Utils.getHash(value), value.length, savedReplicasLst, cryptoKey);
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

        Metadata md = this.mds.tsRead(key, null);
        if (md == null || md.isTombstone()) {
            logger.warn("Could not find metadata associated with key {}.", key);
            return null;
        }

        byte[] value = null;
        String kvsKey = Utils.getKvsKey(key, md.getTs());
        
        if (this.ecEnabled) {   // Erasure coding
            
            ExecutorService executor = Executors.newFixedThreadPool(k);
            CompletionService<Entry<Kvs, byte[]>> compServ = 
                    new ExecutorCompletionService<Entry<Kvs, byte[]>>(executor);
            List<Kvs> kvsLst = this.kvs.getKvsSortedByReadLatency();
            kvsLst.retainAll(md.getReplicasLst());
            @SuppressWarnings("unchecked")
            Future<Entry<Kvs, byte[]>>[] futuresArray = new Future[kvsLst.size()];
            Future<Entry<Kvs, byte[]>> futureResult;
            Entry<Kvs, byte[]> chunk = new AbstractMap.SimpleEntry<Kvs, byte[]>(null, null);
            EcChunk[] chunks = new EcChunk[md.getReplicasLst().size()];
            for (int i=0; i<chunks.length; i++)
                chunks[i] = this.ec.new EcChunk(null, md.getChunksHashes()[i], 
                                                md.getReplicasLst().get(i), ChunkState.KO);
            
            int idxFrom = 0, idxTo = k;
            boolean completed = true;
            int retrieved = 0;
            do {
                List<Kvs> kvsSublst = kvsLst.subList(idxFrom, idxTo);
                for (Kvs kvStore : kvsSublst) {
                    futuresArray[kvsSublst.indexOf(kvStore)] = compServ.submit(this.kvs.new KvsGetWorker(kvStore, kvsKey));
                    for (int j=0; j<chunks.length; j++)
                        if (kvStore.equals(chunks[j].kvs) && 
                                ChunkState.KO.equals(chunks[j].state)) {
                            chunks[j].state = ChunkState.PENDING;
                            break;
                        }
                }
                
                for (int i=0; i<kvsSublst.size(); i++)
                    try {
                        futureResult =  compServ.poll(this.TIMEOUT_READ, TimeUnit.SECONDS);
                        if (futureResult != null && 
                                !(chunk = futureResult.get()).getKey().getId().startsWith(KvsManager.FAIL_PREFIX)) {
                            
                            for (int j=0; j<chunks.length; j++) {
                                if (chunk.getKey().equals(chunks[j].kvs) && 
                                        ChunkState.PENDING.equals(chunks[j].state)) {
                                    if (Arrays.equals(chunks[j].hash, Utils.getHash(chunk.getValue()))) {
                                        chunks[j].state = ChunkState.OK;
                                        chunks[j].data = chunk.getValue();
                                        retrieved++;
                                        logger.debug("Chunk {} retrieved from {}", j, chunks[j].kvs);
                                    } else {
                                        chunks[j].state = ChunkState.KO;
                                        logger.warn("Tampered chunk {} retrieved from {}", j, chunks[j].kvs);
                                    }
                                    break;
                                }
                            }
                         } else if (chunk != null)
                                 for (int j=0; j<chunks.length; j++)
                                     if (chunk.getKey().getId().contains(chunks[i].kvs.getId()))
                                         chunks[i].state = ChunkState.KO;

                    } catch (InterruptedException | ExecutionException e) {
                        logger.warn("Exception on write task execution", e);
                    }
                
                if (retrieved < k) { 
                    completed = false;
                    idxFrom = idxTo;
                    idxTo = this.kvs.getKvsList().size() > idxTo + (k - retrieved)?
                            idxTo + (k - retrieved) : this.kvs.getKvsList().size();
                } else
                    completed = true;
            } while (!completed && idxFrom < idxTo);
            executor.shutdown();
            
            if (retrieved < k) {
                logger.error("Could not retrieve enough chunks for decoding data.");
                return null;
            }
            
            byte[][] dataBlocks = new byte[k][], 
                    codingBlocks = new byte[m][];
            int chunkLen = 0;
            int[] erasures = new int[k+m];
            int idxEr = 0;
            for (int j=0; j<k; j++)
                if (chunks[j].state.equals(ChunkState.OK)){
                    dataBlocks[j] = chunks[j].data;
                    if (chunkLen == 0) chunkLen = chunks[j].data.length;
                } else {
                    erasures[idxEr] = j;
                    idxEr++;
                }
            for (int j=0; j<m; j++)
                if (chunks[k + j].state.equals(ChunkState.OK)) {
                    codingBlocks[j] = chunks[k + j].data;
                    if (chunkLen == 0) chunkLen = chunks[k + j].data.length;
                } else {
                    erasures[idxEr] = k + j;
                    idxEr++;
                }
            
            for (int i = 0; i < idxEr; i++)
                if (erasures[i] < k)
                    dataBlocks[erasures[i]] = new byte[chunkLen];
                else 
                    codingBlocks[erasures[i]-k] = new byte[chunkLen];
            erasures[idxEr] = -1;
            
            value = ec.decode(dataBlocks, codingBlocks, erasures, k, m, md.getSize());
            
            if (md.getCryptoKey() != null)
                try {
                    logger.debug("Decrypting data for key {}", key);
                    value = Utils.decrypt(value, md.getCryptoKey(), this.IV);
                } catch (GeneralSecurityException | UnsupportedEncodingException e) {
                    logger.error("Could not decrypt data", e);
                    throw new HybrisException("Could not decrypt data", e);
                }
            return value;
            
        } else {    // Replication

            if (this.cacheEnabled) {
                value = (byte[]) this.cache.get(kvsKey);
                if (value != null && Arrays.equals(md.getHash(), Utils.getHash(value))) {
    
                    if (md.getCryptoKey() != null)
                        try {
                            logger.debug("Decrypting data for key {}", key);
                            value = Utils.decrypt(value, md.getCryptoKey(), this.IV);
                        } catch (GeneralSecurityException | UnsupportedEncodingException e) {
                            logger.error("Could not decrypt data", e);
                            throw new HybrisException("Could not decrypt data", e);
                        }
    
                    logger.debug("Value of {} retrieved from cache", key);
                    return value;
                }
            }
    
            for (Kvs kvStore : this.kvs.getKvsSortedByReadLatency()) {
    
                if (!md.getReplicasLst().contains(kvStore))
                    continue;
    
                try {
                    // XXX check file size to prevent DOS
                    value = this.kvs.get(kvStore, kvsKey);
                } catch (IOException e) {
                    continue;
                }
    
                if (value != null) {
                    if (Arrays.equals(md.getHash(), Utils.getHash(value))) {
                        logger.info("Value of {} retrieved from kvStore {}", key, kvStore);
                        if (this.cacheEnabled && CachePolicy.ONREAD.equals(this.cachePolicy))
                            this.cache.set(kvsKey, this.cacheExp, value);
    
                        if (md.getCryptoKey() != null)
                            try {
                                logger.debug("Decrypting data for key {}", key);
                                value = Utils.decrypt(value, md.getCryptoKey(), this.IV);
                            } catch (GeneralSecurityException | UnsupportedEncodingException e) {
                                logger.error("Could not decrypt data", e);
                                throw new HybrisException("Could not decrypt data", e);
                            }
    
                        return value;
                    } else {     // The hash doesn't match: Byzantine fault: let's try with the other clouds
                        logger.warn("Tampered data retrieved from {}", kvStore);
                        continue;
                    }
                } else
                    /* This could be due to:
                     * a. Byzantine replicas
                     * b. concurrent gc
                     */
                    return this.parallelGet(key);
            }
    
            return this.parallelGet(key);
        }
    }


    /**
     * Fail-safe parallel GET function.
     * This function gets called whenever the main GET API fails
     * due to Byzantine faults or concurrent GC.
     * @param key
     * @return a byte array containing the value associated with <key>.
     * @throws HybrisException
     */
    private byte[] parallelGet(String key) throws HybrisException {

        HybrisWatcher hwatcher = this.new HybrisWatcher();
        Metadata md = this.mds.tsRead(key, null, hwatcher);
        if (md == null || md.isTombstone()) {
            logger.warn("Could not find metadata associated with key {}.", key);
            return null;
        }

        String kvsKey = Utils.getKvsKey(key, md.getTs());
        ExecutorService executor = Executors.newFixedThreadPool(this.quorum);
        CompletionService<Entry<Kvs, byte[]>> compServ = new ExecutorCompletionService<Entry<Kvs, byte[]>>(executor);
        Future<Entry<Kvs, byte[]>> futureResult;
        byte[] value = null;
        boolean keepRetrieving = true;

        List<Kvs> kvsSublst = this.kvs.getKvsSortedByReadLatency();
        kvsSublst.retainAll(md.getReplicasLst());
        Future<Entry<Kvs, byte[]>>[] futuresArray = new Future[kvsSublst.size()];

        do {
            for (Kvs kvStore : kvsSublst)
                futuresArray[kvsSublst.indexOf(kvStore)] = compServ.submit(this.kvs.new KvsGetWorker(kvStore, kvsKey));

            for (int i=0; i<kvsSublst.size(); i++)
                try {
                    if (hwatcher.isChanged()) {
                        for (Future<Entry<Kvs, byte[]>> future : futuresArray)
                            future.cancel(true);
                        return this.parallelGet(key);
                    }

                    futureResult =  compServ.poll(this.TIMEOUT_READ, TimeUnit.SECONDS);
                    if (futureResult != null && 
                            !futureResult.get().getKey().getId().startsWith(KvsManager.FAIL_PREFIX)) {
                        
                            value = futureResult.get().getValue();
                            if (Arrays.equals(md.getHash(), Utils.getHash(value))) {

                                if (this.cacheEnabled && CachePolicy.ONREAD.equals(this.cachePolicy))
                                    this.cache.set(kvsKey, this.cacheExp, value);
    
                                if (md.getCryptoKey() != null)
                                    try {
                                        logger.debug("Decrypting data for key {}", key);
                                        value = Utils.decrypt(value, md.getCryptoKey(), this.IV);
                                    } catch (GeneralSecurityException | UnsupportedEncodingException e) {
                                        logger.error("Could not decrypt data", e);
                                        throw new HybrisException("Could not decrypt data", e);
                                    }
    
                                keepRetrieving = false;
                                for (Future<Entry<Kvs, byte[]>> future : futuresArray)
                                    future.cancel(true);
                                break;
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    logger.warn("Exception on write task execution", e);
                }

        } while (keepRetrieving);
        executor.shutdown();

        return value;
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

        if (!this.gcEnabled) {
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
        }
        
        Timestamp ts = md.getTs();
        ts.inc( this.clientId );
        Metadata tombstone = Metadata.getTombstone(ts);
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
     * XXX not scalable: for debugging purposes
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
    private class HybrisWatcher implements CuratorWatcher {

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
}
