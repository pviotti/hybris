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
package fr.eurecom.hybris.kvs;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.jclouds.rest.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.drivers.AmazonKvs;
import fr.eurecom.hybris.kvs.drivers.AzureKvs;
import fr.eurecom.hybris.kvs.drivers.FaultyKvs;
import fr.eurecom.hybris.kvs.drivers.GoogleKvs;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.kvs.drivers.RackspaceKvs;
import fr.eurecom.hybris.kvs.drivers.TransientKvs;


/**
 * KvsManager exposes a simple common cloud storage API
 * for saving, retrieving and deleting data
 * on the supported cloud storage services.
 * @author p.viotti
 */
public class KvsManager {

    private static final Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private final List<Kvs> kvsLst;                     // kvStores list (not sorted)
    private final List<Kvs> kvsLstByReads;              // kvStores sorted by read latency
    private final List<Kvs> kvsLstByWrites;             // kvStores sorted by write latency

    private static final int LATENCY_TEST_DATA_SIZE = 100;     // default value: 100kB
    
    public static final String FAIL_PREFIX = "FAIL-";

    public enum KvsId {
        AMAZON((short) 0),
        AZURE((short) 1),
        GOOGLE((short) 2),
        RACKSPACE((short) 3),
        TRANSIENT((short) 4),
        FAULTY((short) 5);

        private short serialNum;

        private KvsId(short sn) {
            this.serialNum = sn;
        }

        public int getSerial() {
            return this.serialNum;
        }

        public static KvsId getIdFromSerial(short num) {
            switch (num) {
                case 0: return AMAZON;
                case 1: return AZURE;
                case 2: return GOOGLE;
                case 3: return RACKSPACE;
                case 4: return TRANSIENT;
                case 5: return FAULTY;
                default: throw new IllegalArgumentException();
            }
        }

        public String toString() {
            return super.toString().toLowerCase();
        }
    };

    public KvsManager(String accountsFile, String container, boolean testLatency) throws IOException {

    	Config conf = Config.getInstance();
        conf.loadAccountsProperties(accountsFile);

        this.kvsLst = new ArrayList<Kvs>();
        this.kvsLstByReads = new ArrayList<Kvs>();
        this.kvsLstByWrites = new ArrayList<Kvs>();

        String[] accountIds = conf.getAccountsIds();

        Kvs kvStore;
        String accessKey, secretKey;
        boolean enabled;
        int cost;

        for (String accountId : accountIds) {
            accessKey = conf.getAccountsProperty( String.format(Config.C_AKEY, accountId) );
            secretKey = conf.getAccountsProperty( String.format(Config.C_SKEY, accountId) );
            enabled = Boolean.parseBoolean( conf.getAccountsProperty( String.format(Config.C_ENABLED, accountId)) );
            cost = Integer.parseInt( conf.getAccountsProperty( String.format(Config.C_COST, accountId) ));

            try {
                switch (KvsId.valueOf(accountId.toUpperCase())) {
                    case AMAZON:
                        kvStore = new AmazonKvs(accountId, accessKey, secretKey,
                                container, enabled, cost);
                        break;
                    case AZURE:
                        kvStore = new AzureKvs(accountId, accessKey, secretKey,
                                container, enabled, cost);
                        break;
                    case GOOGLE:
                        kvStore = new GoogleKvs(accountId, accessKey, secretKey,
                                container, enabled, cost);
                        break;
                    case RACKSPACE:
                        kvStore = new RackspaceKvs(accountId, accessKey, secretKey,
                                container, enabled, cost);
                        break;
                    case TRANSIENT:
                        kvStore = new TransientKvs(accountId, accessKey, secretKey,
                                container, enabled, cost);
                        break;
                    case FAULTY:
                        kvStore = new FaultyKvs(accountId, accessKey, secretKey,
                                container, enabled, cost);
                        break;
                    default:
                        logger.error("Hybris could not find any driver for {} KvStore", accountId);
                        continue;
                }
            } catch (IllegalArgumentException e) {
                logger.error("Hybris could not find any driver for {} KvStore", accountId);
                throw new IOException(e);
            }

            this.kvsLst.add(kvStore);
        }

        this.kvsLstByReads.addAll(this.kvsLst);
        this.kvsLstByWrites.addAll(this.kvsLst);

        if (testLatency)
            this.testLatencyAndSortClouds(LATENCY_TEST_DATA_SIZE);
    }


    public List<Kvs> getKvsList()                   { return this.kvsLst; }
    public List<Kvs> getKvsSortedByReadLatency()    { return this.kvsLstByReads; }
    public List<Kvs> getKvsSortedByWriteLatency()   { return this.kvsLstByWrites; }


    /**
     * Worker thread class in charge of asynchronously performing
     * write operations on cloud stores.
     * @author p.viotti
     */
    public class KvsPutWorker implements Callable<Kvs> {

        private final Kvs kvStore;
        private final String key;
        private final byte[] value;

        public KvsPutWorker(Kvs kvStore, String key, byte[] value) {
            this.kvStore = kvStore;
            this.key = key;
            this.value = value;
        }

        public Kvs call() {
            try {
                KvsManager.this.put(this.kvStore, this.key, this.value);
                return this.kvStore;
            } catch (Exception e) {
                return new Kvs(FAIL_PREFIX + kvStore.getId(), "", false, 0);
            }
        }
    }


    /**
     * Worker thread class in charge of asynchronously performing
     * read operations on cloud stores.
     * @author p.viotti
     */
    public class KvsGetWorker implements Callable<Entry<Kvs, byte[]>> {

        private final Kvs kvStore;
        private final String key;

        public KvsGetWorker(Kvs kvStore, String key) {
            this.kvStore = kvStore;
            this.key = key;
        }

        public Entry<Kvs, byte[]> call() {
            try {
                byte[] result = KvsManager.this.get(this.kvStore, this.key);
                if (result == null)
                    throw new Exception();
                return new AbstractMap.SimpleEntry<Kvs, byte[]>(kvStore, result);
            } catch (Exception e) {
                return new AbstractMap.SimpleEntry<Kvs, byte[]>
                (new Kvs(FAIL_PREFIX + kvStore.getId(), null, false, 0) , null);
            }
        }
    }
    

    /**
     * Worker thread class in charge of testing read and
     * write latencies of a KvStore.
     * @author p.viotti
     */
    public class LatencyTester implements Runnable {

        private final Kvs kvStore;

        private static final String TEST_KEY = "latency_test-";
        private final byte[] testData;

        public LatencyTester(Kvs kvStore, int testDataSize) {
            this.kvStore = kvStore;
            if (testDataSize == 0)
                testDataSize = KvsManager.LATENCY_TEST_DATA_SIZE;
            this.testData = Utils.generateRandomBytes(new byte[testDataSize]);
        }

        public void run() {

            String testKey = TEST_KEY + Utils.random.nextInt(1000);
            long start, end = 0;

            // Write
            try {
                start = System.currentTimeMillis();
                KvsManager.this.put(this.kvStore, testKey, this.testData);
                end = System.currentTimeMillis();
                this.kvStore.setWriteLatency(end - start);
            } catch (Exception e) {
                this.kvStore.setWriteLatency(Integer.MAX_VALUE);
                if (e instanceof AuthorizationException)
                    this.kvStore.setEnabled(false);
                return;
            }

            // Read
            byte[] retrieved = null;
            try {
                start = System.currentTimeMillis();
                retrieved = KvsManager.this.get(this.kvStore, testKey);
                end = System.currentTimeMillis();
            } catch (Exception e) {
                this.kvStore.setReadLatency(Integer.MAX_VALUE);
                if (e instanceof AuthorizationException)
                    this.kvStore.setEnabled(false);
            }
            if (retrieved == null || !Arrays.equals(this.testData, retrieved))
                this.kvStore.setReadLatency(Integer.MAX_VALUE);
            else
                this.kvStore.setReadLatency(end - start);

            // Clean up
            try {
                KvsManager.this.delete(this.kvStore, testKey);
            } catch (IOException e) {  }
        }
    }


    /* ---------------------------------------------------------------------------------------
                                            Public APIs
       --------------------------------------------------------------------------------------- */


    public void put(Kvs kvStore, String key, byte[] data) throws IOException {
        try {
            kvStore.put(key, data);
        } catch (IOException e) {
            logger.warn("Could not put " + key + " on " + kvStore, e);
            throw e;
        }
    }


    public byte[] get(Kvs kvStore, String key) throws IOException {
        try {
            byte[] value = kvStore.get(key);
            if (value == null)
                logger.warn("Could not find key {} in {}", key, kvStore);
            return value;
        } catch (IOException e) {
            logger.warn("Could not get " + key + " from " + kvStore, e);
            throw e;
        }
    }


    public void delete(Kvs kvStore, String key) throws IOException {
        try {
            kvStore.delete(key);
        } catch (IOException e) {
            logger.warn("Could not delete " + key + " from " + kvStore, e);
            throw e;
        }
    }


    public List<String> list(Kvs kvStore) throws IOException {
        try {
            return kvStore.list();
        } catch (IOException e) {
            logger.warn("Could not list keys in {}", kvStore, e);
            throw e;
        }
    }


    public void shutdown(Kvs kvStore) {
        try {
            kvStore.shutdown();
        } catch (IOException e) {
            logger.warn("Error during {} KvStore shutdown", kvStore, e);
        }
    }


    public void testLatencyAndSortClouds(int testDataSize) {

        logger.info("Performing {} kB latency tests on cloud kvStores..", testDataSize);
        this.testLatency(testDataSize);

        Collections.sort(this.kvsLstByReads, Kvs.COMPARATOR_BY_READS);
        Collections.sort(this.kvsLstByWrites, Kvs.COMPARATOR_BY_WRITES);

        if (logger.isDebugEnabled()) {
            logger.debug("Cloud kvStores sorted by write latency:");
            for(Kvs kvs : this.kvsLstByWrites)
                logger.debug("\t* {}", kvs.toVerboseString());
            logger.debug("Cloud kvStores sorted by read latency:");
            for(Kvs kvs : this.kvsLstByReads)
                logger.debug("\t* {}", kvs.toVerboseString());
        }
    }


    /**
     * Empty the data storage root container.
     * ATTENTION: it erases all data stored in the root container!
     * @param kvStore the cloud storage provider
     * @throws IOException
     */
    public void emptyStorageContainer(Kvs kvStore) throws IOException {
        logger.debug("Listing {}", kvStore);
        List<String> keys = kvStore.list();
        for (String key : keys)
            try {
                logger.debug("Removing {} from {}", key, kvStore);
                kvStore.delete(key);
            } catch (IOException e) { }
    }

    /* ---------------------------------------------------------------------------------------
                                        Private methods
       --------------------------------------------------------------------------------------- */


    private void testLatency(int testDataSize) {
        ExecutorService executor = Executors.newFixedThreadPool(this.kvsLst.size());
        List<FutureTask<Object>> futureLst = new ArrayList<FutureTask<Object>>(this.kvsLst.size());
        for (Kvs kvStore : this.kvsLst) {
            FutureTask<Object> f = new FutureTask<Object>(new LatencyTester(kvStore, testDataSize), null);
            futureLst.add(f);
            executor.execute(f);
        }

        for (FutureTask<Object> future : futureLst)
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                logger.warn("Exception while running latency test.", e);
            }

        executor.shutdown();
    }
}