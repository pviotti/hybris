package fr.eurecom.hybris.kvs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import org.jclouds.rest.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.kvs.drivers.AmazonKvs;
import fr.eurecom.hybris.kvs.drivers.AzureKvs;
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

    private final Config conf;
    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private final List<Kvs> kvStores;      // kvStores sorted by cost and latency

    private static String rootContainer;

    private static String TEST_KEY = "latency_test-";
    private static String TEST_VALUE = "1234567890QWERTYUIOPASDFGHJKLZXCVBNM";

    private enum KvsId {
        AMAZON,
        AZURE,
        GOOGLE,
        RACKSPACE,
        TRANSIENT;
    };

    public KvsManager(String accountsFile, String container, boolean testLatency) throws IOException {

        rootContainer = container;

        this.kvStores = Collections.synchronizedList(new ArrayList<Kvs>());
        this.conf = Config.getInstance();
        this.conf.loadAccountsProperties(accountsFile);
        String[] accountIds = this.conf.getAccountsIds();

        Kvs kvStore;
        String accessKey, secretKey;
        boolean enabled;
        int cost;

        for (String accountId : accountIds) {
            accessKey = this.conf.getAccountsProperty( String.format(Config.C_AKEY, accountId));
            secretKey = this.conf.getAccountsProperty( String.format(Config.C_SKEY, accountId) );
            enabled = Boolean.parseBoolean( this.conf.getAccountsProperty( String.format(Config.C_ENABLED, accountId)) );
            cost = Integer.parseInt( this.conf.getAccountsProperty( String.format(Config.C_COST, accountId) ));

            switch (KvsId.valueOf(accountId.toUpperCase())) {       // TODO what happens if does not find a value?
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
                default:
                    logger.error("Hybris could not find driver {}", accountId); // TODO
                    continue;
            }
            this.kvStores.add(kvStore);
        }

        if (testLatency) {
            logger.info("Performing latency tests on cloud kvStores...");
            this.testLatency();
            Collections.sort(this.kvStores);  // Sort kvStores according to cost and latency (see Driver.compareTo())
            logger.debug("Cloud kvStores sorted by performance/cost metrics:");
            synchronized (this.kvStores) {
                for(Kvs kvs : this.kvStores)
                    logger.debug("\t* {}", kvs.toVerboseString());
            }
        }
    }


    public List<Kvs> getKvStores()   { return this.kvStores; }

    /**
     * Worker thread class in charge of asynchronously performing
     * write operation on cloud stores.
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

        @Override
        public Kvs call() {
            try {
                KvsManager.this.put(this.kvStore, this.key, this.value);
                return this.kvStore;
            } catch (Exception e) {
                logger.warn("Could not put " + this.key + " on " + this.kvStore.getId(), e);
                return null;
            }
        }
    }


    /* ---------------------------------------------------------------------------------------
                                            Public APIs
       --------------------------------------------------------------------------------------- */


   public void put(Kvs kvStore, String key, byte[] data) throws IOException {
       if (!kvStore.isAlreadyUsed())
           if (kvStore.createContainer())
               logger.debug("Created root data container \"{}\" for {}", rootContainer, kvStore.getId());
       kvStore.put(key, data);
    }


    public byte[] get(Kvs kvStore, String key) throws IOException {
        return kvStore.get(key);
    }


    public void delete(Kvs kvStore, String key) throws IOException {
        kvStore.delete(key);
    }


    public List<String> list(Kvs kvStore) throws IOException {
        return kvStore.list();
    }


    /**
     * Empty the data storage root container.
     * ATTENTION: it erases all data stored in the root container!
     * @param kvStore the cloud storage provider
     * @throws IOException
     */
    public void emptyStorageContainer(Kvs kvStore) throws IOException {
        List<String> keys = kvStore.list();
        for (String key : keys)
            kvStore.delete(key);
    }

    /* ---------------------------------------------------------------------------------------
                                        Private methods
       --------------------------------------------------------------------------------------- */


    private void testLatency() {    // TODO check, parallelize

        byte[] testData = TEST_VALUE.getBytes();
        String testKey = TEST_KEY + new Random().nextInt(1000);
        long start, end = 0;

        synchronized(this.kvStores){
            for (Kvs kvStore : this.kvStores) {

                // write
                try {
                    start = System.currentTimeMillis();
                    this.put(kvStore, testKey, testData);
                    end = System.currentTimeMillis();
                    kvStore.setWriteLatency(end - start);
                } catch (Exception e) {
                    logger.error("error while storing " + testKey + " on " + kvStore.getId(), e);
                    kvStore.setWriteLatency(Integer.MAX_VALUE);
                    if (e instanceof AuthorizationException)
                        kvStore.setEnabled(false);
                        continue;
                }

                // read
                byte[] retrieved = null;
                try {
                    start = System.currentTimeMillis();
                    retrieved = this.get(kvStore, testKey);
                    end = System.currentTimeMillis();
                } catch (Exception e) {
                    logger.error("error while reading " + testKey + " on " + kvStore.getId(), e);
                    kvStore.setReadLatency(Integer.MAX_VALUE);
                    if (e instanceof AuthorizationException)
                        kvStore.setEnabled(false);
                }
                if (retrieved == null || !Arrays.equals(testData, retrieved))
                    kvStore.setReadLatency(Integer.MAX_VALUE);
                else
                    kvStore.setReadLatency(end - start);

                // clean up
                try {
                    this.delete(kvStore, testKey);
                } catch (IOException e) {
                    logger.warn("Could not remove {} from {}.", testKey, kvStore.getId());
                }
            }
        }
    }
}