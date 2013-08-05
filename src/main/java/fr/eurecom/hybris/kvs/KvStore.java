package fr.eurecom.hybris.kvs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStores;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.rest.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

import fr.eurecom.hybris.Config;

/**
 * KvStore exposes a simple common cloud storage API
 * for saving, retrieving and deleting data
 * on the supported cloud storage services.
 * @author p.viotti
 */
public class KvStore {

    private final Config conf;
    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private final List<CloudProvider> providers;      // storage providers sorted by cost and latency

    private static String rootContainer;

    private static String TEST_KEY = "latency_test-";
    private static String TEST_VALUE = "1234567890QWERTYUIOPASDFGHJKLZXCVBNM";

    public KvStore(String accountsFile, String container, boolean testLatency) throws IOException {

        rootContainer = container;

        this.providers = Collections.synchronizedList(new ArrayList<CloudProvider>());
        this.conf = Config.getInstance();
        this.conf.loadAccountsProperties(accountsFile);
        String[] accountIds = this.conf.getAccountsIds();
        for (String accountId : accountIds)
            this.providers.add(new CloudProvider(accountId,
                            this.conf.getAccountsProperty( String.format(Config.C_AKEY, accountId) ),
                            this.conf.getAccountsProperty( String.format(Config.C_SKEY, accountId) ),
                            Boolean.parseBoolean( this.conf.getAccountsProperty( String.format(Config.C_ENABLED, accountId)) ),
                            Integer.parseInt( this.conf.getAccountsProperty( String.format(Config.C_COST, accountId) ) )));

        if (testLatency) {
            logger.info("Performing latency tests on cloud providers...");
            this.testLatency();
            Collections.sort(this.providers);  // Sort providers according to cost and latency (see CloudProvider.compareTo())
            logger.debug("Cloud providers sorted by performance/cost metrics:");
            synchronized (this.providers) {
                for(CloudProvider provider : this.providers)
                    logger.debug("\t* " + provider.toVerboseString());
            }
        }
    }


    public List<CloudProvider> getProviders()   { return this.providers; }


    /**
     * Worker thread class in charge of asynchronously performing
     * write operation on cloud stores.
     * @author p.viotti
     */
    public class KvsPutWorker implements Callable<CloudProvider> {

        private final CloudProvider provider;
        private final String key;
        private final byte[] value;

        public KvsPutWorker(CloudProvider provider, String key, byte[] value) {
            this.provider = provider;
            this.key = key;
            this.value = value;
        }

        @Override
        public CloudProvider call() {
            try {
                KvStore.this.put(this.provider, this.key, this.value);
                return this.provider;
            } catch (Exception e) {
                logger.warn("Could not put " + this.key + " on " + this.provider.getId(), e);
                return null;
            }
        }
    }


    /* ---------------------------------------------------------------------------------------
                                            Public APIs
       --------------------------------------------------------------------------------------- */


   public void put(CloudProvider provider, String key, byte[] data) throws IOException {

        try {
            BlobStore storage = provider.getBlobStore();
            if (!provider.isAlreadyUsed()) {
                boolean created = storage.createContainerInLocation(null, rootContainer);
                provider.setAlreadyUsed(true);
                if (created)
                    logger.debug("Created root data container \"{}\" for {}", rootContainer, provider.getId());
            }

            // logger.debug("Storing {} on {}...", key, provider.getId());
            Blob blob = storage.blobBuilder(key).payload(data).build();
            storage.putBlob(rootContainer, blob); // multipart() for enabling multipart uploads (if supported)
            // logger.debug("Finished storing {} on {}.", key, provider.getId());
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }


    public byte[] get(CloudProvider provider, String key) throws IOException {

        try {
            BlobStore storage = provider.getBlobStore();
            Blob blob = storage.getBlob(rootContainer, key);
            if (blob == null) {
                logger.warn("Could not find key {} in {}", key, provider.getId());
                return null;
            }
            return ByteStreams.toByteArray(blob.getPayload());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }


    public void delete(CloudProvider provider, String key) throws IOException {

        try {
            BlobStore storage = provider.getBlobStore();
            storage.removeBlob(rootContainer, key);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }


    public List<String> list(CloudProvider provider) throws IOException {

        try {
            List<String> keys = new ArrayList<String>();
            BlobStore storage = provider.getBlobStore();

            for (StorageMetadata resourceMd : BlobStores.listAll(storage, rootContainer, ListContainerOptions.NONE))
                keys.add(resourceMd.getName());
            return keys;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }


    /**
     * Empty the data storage root container.
     * ATTENTION: it erases all data stored in the root container!
     * @param provider the cloud storage provider
     * @throws IOException
     */
    public void emptyStorageContainer(CloudProvider provider) throws IOException {

        try {
            List<String> keys = this.list(provider);
            for (String key : keys)
                this.delete(provider, key);
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    /* ---------------------------------------------------------------------------------------
                                        Private methods
       --------------------------------------------------------------------------------------- */


    private void testLatency() {    // TODO parallelize

        byte[] testData = TEST_VALUE.getBytes();
        String testKey = TEST_KEY + new Random().nextInt(1000);
        long start, end = 0;

        synchronized(this.providers){
            for (CloudProvider provider : this.providers) {

                // write
                try {
                    start = System.currentTimeMillis();
                    this.put(provider, testKey, testData);
                    end = System.currentTimeMillis();
                    provider.setWriteLatency(end - start);
                } catch (Exception e) {
                    logger.error("error while storing " + testKey + " on " + provider.getId(), e);
                    provider.setWriteLatency(Integer.MAX_VALUE);
                    if (e instanceof AuthorizationException)
                        provider.setEnabled(false);
                    continue;
                }

                // read
                byte[] retrieved = null;
                try {
                    start = System.currentTimeMillis();
                    retrieved = this.get(provider, testKey);
                    end = System.currentTimeMillis();
                } catch (Exception e) {
                    logger.error("error while reading " + testKey + " on " + provider.getId(), e);
                    provider.setReadLatency(Integer.MAX_VALUE);
                    if (e instanceof AuthorizationException)
                        provider.setEnabled(false);
                }
                if (retrieved == null || !Arrays.equals(testData, retrieved))
                    provider.setReadLatency(Integer.MAX_VALUE);
                else
                    provider.setReadLatency(end - start);

                // clean up
                try {
                    this.delete(provider, testKey);
                } catch (IOException e) {
                    logger.warn("Could not remove {} from {}.", testKey, provider.getId());
                }
            }
        }
    }
}