package fr.eurecom.hybris.kvs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
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
    
    private Config conf;
    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);
    
    private List<CloudProvider> providers;      // storage providers sorted by cost and latency
    
    private static String rootContainer;
    
    private static String TEST_KEY = "latency_test-";
    private static String TEST_VALUE = "1234567890QWERTYUIOPASDFGHJKLZXCVBNM";
    
    public KvStore(String container, boolean testLatency) throws IOException {
        
        conf = Config.getInstance();
        
        rootContainer = container;
        
        this.providers = Collections.synchronizedList(new ArrayList<CloudProvider>());
        String[] accountIds = conf.getAccountsIds();
        for (String accountId : accountIds) {
            providers.add(new CloudProvider(accountId, 
                            conf.getAccountsProperty( String.format(Config.C_AKEY, accountId) ), 
                            conf.getAccountsProperty( String.format(Config.C_SKEY, accountId) ),
                            Boolean.parseBoolean( conf.getAccountsProperty( String.format(Config.C_ENABLED, accountId)) ),
                            Integer.parseInt( conf.getAccountsProperty( String.format(Config.C_COST, accountId) ) )));
        }
        
        if (testLatency) {
            logger.info("Performing latency tests on cloud providers...");
            testLatency();
            Collections.sort(this.providers);  // Sort providers according to cost and latency (see CloudProvider.compareTo())
            logger.debug("Cloud providers sorted by performance/cost metrics:");
            synchronized (this.providers) {
                for(CloudProvider provider : this.providers) 
                    logger.debug("\t* " + provider.toString());
            }
        }
    }
    
    
    public List<CloudProvider> getProviders()   { return providers; }
    
    
    /**
     * Worker thread class in charge of asynchronously performing 
     * write operation on cloud stores. 
     * @author p.viotti
     */
    public class KvsPutWorker implements Callable<CloudProvider> {
        
        private CloudProvider provider;
        private String key;
        private byte[] value;
        
        public KvsPutWorker(CloudProvider provider, String key, byte[] value) {
            this.provider = provider;
            this.key = key;
            this.value = value;
        }

        @Override
        public CloudProvider call() {
            try {
                put(provider, key, value);
                return provider;
            } catch (Exception e) {
                logger.warn("Could not put " + key + " on " + provider.getId(), e);
                return null;
            }
        }
    }

    
    /* ---------------------------------------------------------------------------------------
                                            Public APIs
       --------------------------------------------------------------------------------------- */
    

   public void put(CloudProvider provider, String key, byte[] data) throws IOException {
        
        BlobStoreContext context = null; 
        BlobStore storage = null; 
        Blob blob = null;
        
        try {
            context = ContextBuilder.newBuilder(provider.getId())
                                    .credentials(provider.getAccessKey(), provider.getSecretKey())
                                    .buildView(BlobStoreContext.class);
            storage = context.getBlobStore();
            
            if (!provider.isAlreadyUsed()) {
                boolean created = storage.createContainerInLocation(null, rootContainer);
                provider.setAlreadyUsed(true);
                if (created)
                    logger.debug("Created root data container \"{}\" for {}", rootContainer, provider.getId());
            }
            
            // logger.debug("Storing {} on {}...", key, provider.getId());
            blob = storage.blobBuilder(key).payload(data).build();
            storage.putBlob(rootContainer, blob);
            // logger.debug("Finished storing {} on {}.", key, provider.getId());
            
        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            if (context != null)
                context.close();
        }
    }


    public byte[] get(CloudProvider provider, String key) throws IOException {
        
        BlobStoreContext context = null; 
        BlobStore storage = null; 
        Blob blob = null;
        
        try {
            context = ContextBuilder.newBuilder(provider.getId())
                                    .credentials(provider.getAccessKey(), provider.getSecretKey())
                                    .buildView(BlobStoreContext.class);
            storage = context.getBlobStore();
            blob = storage.getBlob(rootContainer, key);
            if (blob == null) {
                logger.warn("Could not find key {} in {}", key, provider.getId());
                return null;
            }
            return ByteStreams.toByteArray(blob.getPayload());
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (context != null)
                context.close();
        }
    }


    public void delete(CloudProvider provider, String key) throws IOException {
        
        BlobStoreContext context = null; 
        BlobStore storage = null; 
        
        try {
            context = ContextBuilder.newBuilder(provider.getId())
                                    .credentials(provider.getAccessKey(), provider.getSecretKey())
                                    .buildView(BlobStoreContext.class);

            storage = context.getBlobStore();
            storage.removeBlob(rootContainer, key);

        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            if (context != null)
                context.close();
        }
    }
    
    
    public List<String> list(CloudProvider provider) throws IOException {
        
        BlobStoreContext context = null; 
        BlobStore storage = null; 
        List<String> keys = new ArrayList<String>();
        
        try {
            context = ContextBuilder.newBuilder(provider.getId())
                                    .credentials(provider.getAccessKey(), provider.getSecretKey())
                                    .buildView(BlobStoreContext.class);

            storage = context.getBlobStore();
            for (StorageMetadata resourceMd : storage.list(rootContainer))  // TODO this list might just be a partial PageSet
                keys.add(resourceMd.getName());
            return keys;
        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            if (context != null)
                context.close();
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
        String testKey = TEST_KEY + (new Random()).nextInt(1000);
        long start, end = 0;
        
        synchronized(this.providers){
            for (CloudProvider provider : this.providers) {
                
                // write
                try {
                    start = System.currentTimeMillis();
                    put(provider, testKey, testData);
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
                    retrieved = get(provider, testKey);
                    end = System.currentTimeMillis();
                } catch (Exception e) {
                    logger.error("error while reading " + testKey + " on " + provider.getId(), e);
                    provider.setReadLatency(Integer.MAX_VALUE);
                    if (e instanceof AuthorizationException)
                        provider.setEnabled(false);
                }
                if ((retrieved == null) || (!Arrays.equals(testData, retrieved)))
                    provider.setReadLatency(Integer.MAX_VALUE);
                else
                    provider.setReadLatency(end - start);
                
                // clean up
                try {
                    delete(provider, testKey);
                } catch (IOException e) {
                    logger.warn("Could not remove {} from {}.", testKey, provider.getId());
                }
            }
        }
    }
}