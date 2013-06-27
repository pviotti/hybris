package fr.eurecom.hybris.kvs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
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
    
    private Config conf = Config.getInstance();
    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);
    
    private List<CloudProvider> providers;      // storage providers sorted by cost and latency
    
    private String rootContainer;
    
    private static String TEST_KEY = "latency_test-";
    private static String TEST_VALUE = "1234567890QWERTYUIOPASDFGHJKLZXCVBNM";
    
    private static String KEY_NOT_FOUND_MSG = "Key not found";
    
    public KvStore(String rootContainer, boolean testLatency) {
        
        this.rootContainer = rootContainer;
        
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
                storage.createContainerInLocation(null, rootContainer);
                provider.setAlreadyUsed(true);
                logger.debug("Created container {} for {}", rootContainer, provider.getId());
            }
            
            blob = storage.blobBuilder(key).payload(data).build();
            storage.putBlob(rootContainer, blob);
            
        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            if (context != null)
                context.close();
        }
    }


    public byte[] get(CloudProvider provider, String key) {
        
        BlobStoreContext context = null; 
        BlobStore storage = null; 
        Blob blob = null;
        
        try {
            context = ContextBuilder.newBuilder(provider.getId())
                                    .credentials(provider.getAccessKey(), provider.getSecretKey())
                                    .buildView(BlobStoreContext.class);
            storage = context.getBlobStore();
            blob = storage.getBlob(rootContainer, key);
            if (blob == null) throw new IOException(KEY_NOT_FOUND_MSG);      // key not found
            return ByteStreams.toByteArray(blob.getPayload());
        } catch (IOException e) {
            if (KEY_NOT_FOUND_MSG.equalsIgnoreCase(e.getMessage()))
                logger.warn("Could not find key {} in {}", key, provider.getId());
            else
                logger.error("Error while retrieving " + key + " from " + provider.getId(), e);
            return null;
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


    /* ---------------------------------------------------------------------------------------
                                        Private methods
       --------------------------------------------------------------------------------------- */
    

    private void testLatency() {
        
        byte[] testData = TEST_VALUE.getBytes();
        String testKey = TEST_KEY + (new Random()).nextInt(1000);
        long start, end = 0;
        
        // Perform write tests
        
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
                start = System.currentTimeMillis();
                byte[] retrieved = get(provider, testKey);
                end = System.currentTimeMillis();
                if ((retrieved == null) || (!Arrays.equals(testData, retrieved))) {
                    provider.setReadLatency(Integer.MAX_VALUE);
                } else
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