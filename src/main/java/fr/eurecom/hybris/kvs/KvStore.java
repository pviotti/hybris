package fr.eurecom.hybris.kvs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    
    private Config config = Config.getInstance();
    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);
    
    private Map<String, CloudProvider> providers;
    private List<CloudProvider> sortedProviders;      // storage providers sorted by cost and latency
    
    private String rootContainer;
    private int quorum;
    
    private String TEST_KEY = "latency_test-";
    private String TEST_VALUE = "1234567890QWERTYUIOPASDFGHJKLZXCVBNM";
    
    public KvStore(String rootContainer, boolean latencyTests) {
        
        this.rootContainer = rootContainer;
        
        this.providers = Collections.synchronizedMap(new HashMap<String, CloudProvider>());
        String[] accountIds = config.getAccountsIds();
        for (String accountId : accountIds) {
            providers.put(accountId, 
                    new CloudProvider(accountId, 
                            config.getAccountsProperty( String.format(Config.C_AKEY, accountId) ), 
                            config.getAccountsProperty( String.format(Config.C_SKEY, accountId) ),
                            Boolean.parseBoolean( config.getAccountsProperty( String.format(Config.C_ENABLED, accountId)) ),
                            Integer.parseInt( config.getAccountsProperty( String.format(Config.C_COST, accountId) ) )));
        }
        
        
        int t = Integer.parseInt(config.getProperty(Config.CONST_T));
        this.quorum = t + 1;
        
        this.sortedProviders = new ArrayList<CloudProvider>(providers.values());
        if (latencyTests) {
            logger.info("Performing latency tests on cloud providers...");
            performLatencyTests();
            Collections.sort(sortedProviders);  // Sort providers according to cost and latency (see CloudProvider.compareTo())
            logger.debug("Clouds providers sorted by performance/cost metrics:");
            for(CloudProvider cloud : sortedProviders) 
                logger.debug("\t * " + cloud.toString());
        }
    }    

    // =======================================================================================
    //                                      PUBLIC APIs
    // ---------------------------------------------------------------------------------------
    
    public List<String> put(String key, byte[] data) {
        
        List<String> savedKvsLst = new ArrayList<String>();
        
        synchronized (providers) {
            for (CloudProvider provider : sortedProviders) {
                try {
                    putInCloud(provider, key, data);                    
                    savedKvsLst.add(provider.getId());
                    if (savedKvsLst.size() >= this.quorum) break;
                } catch (Exception e) {
                    logger.error("error while storing " + key + " on " + provider.getId(), e);
                }
            }
        }
        
        if (savedKvsLst.size() < this.quorum) {
            // TODO start a new thread to garbage collect the copies of the data on savedKvsLst
            return null;
        } else        
            return savedKvsLst;
    }
    
    public byte[] getFromCloud(String provider, String key) {
        
        BlobStoreContext context = null; 
        BlobStore storage = null; 
        Blob blob = null;
        
        CloudProvider cloud = providers.get(provider);
        try {
            context = ContextBuilder.newBuilder(cloud.getId())
                                    .credentials(cloud.getAccessKey(), cloud.getSecretKey())
                                    .buildView(BlobStoreContext.class);
            storage = context.getBlobStore();
            blob = storage.getBlob(rootContainer, key);
            if (blob == null) throw new IOException(); // key not found
            return ByteStreams.toByteArray(blob.getPayload());
        } catch (Exception e) {
            logger.error("error while retrieving " + key + " on " + cloud.getId(), e);
            return null;
        } finally {
            if (context != null)
                context.close();
        }
    }
    
    public void deleteKeyFromCloud(String provider, String key) throws IOException {
        
        BlobStoreContext context = null; 
        BlobStore storage = null; 
        
        CloudProvider cloud = providers.get(provider);
        try {
            context = ContextBuilder.newBuilder(cloud.getId())
                                    .credentials(cloud.getAccessKey(), cloud.getSecretKey())
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
    
    // =======================================================================================
    //                                 PRIVATE METHODS
    // ---------------------------------------------------------------------------------------
    
    private void putInCloud(CloudProvider provider, String key, byte[] data) throws Exception {
        
        BlobStoreContext context = null; 
        BlobStore storage = null; 
        Blob blob = null;
        
        context = ContextBuilder.newBuilder(provider.getId())
                                .credentials(provider.getAccessKey(), provider.getSecretKey())
                                .buildView(BlobStoreContext.class);
        storage = context.getBlobStore();
        
        if (!provider.isAlreadyUsed()) {
            storage.createContainerInLocation(null, rootContainer);
            provider.setAlreadyUsed(true);
            logger.debug("created container {} for provider {}", rootContainer, provider.getId());
        }
        
        blob = storage.blobBuilder(key).payload(data).build();
        storage.putBlob(rootContainer, blob);
    }
    
//    private void deleteContainerFromCloud(String provider, String container) {
//        // TODO
//    }
    
    private void performLatencyTests() {
        
        byte[] testData = TEST_VALUE.getBytes();
        String testKey = TEST_KEY + (new Random()).nextInt(1000);
        long start, end = 0;
        
        // Perform write tests
        for (CloudProvider provider : sortedProviders) {
            try {
                start = System.currentTimeMillis();
                putInCloud(provider, testKey, testData);
                end = System.currentTimeMillis();
                provider.setWriteLatency(end - start);
            } catch (Exception e) {
                logger.error("error while storing " + testKey + " on " + provider.getId(), e);
                provider.setWriteLatency(999999);
                if (e instanceof AuthorizationException)
                    provider.setEnabled(false); // XXX or remove the provider?
            }
        }
        
        // Perform read tests
        for (CloudProvider provider : sortedProviders) {
            if (provider.getWriteLatency() == -1) continue;     // could not write, so do not perform reading test
            try {
                start = System.currentTimeMillis();
                byte[] retrieved = getFromCloud(provider.getId(), testKey);
                end = System.currentTimeMillis();
                if (!Arrays.equals(testData, retrieved)) {
                    logger.warn("retrieved blob does not match original data: {}", 
                                    retrieved == null ? "null" : new String(retrieved));
                    provider.setReadLatency(999999);
                } else
                    provider.setReadLatency(end - start);
            } catch (Exception e) {
                logger.error("error while retrieving " + testKey + " on " + provider.getId(), e);
                provider.setReadLatency(999999);
                if (e instanceof AuthorizationException)
                    provider.setEnabled(false);
            }
        }
        
        // Clean up test data
        for (CloudProvider provider : sortedProviders) {
            if (provider.getWriteLatency() == -1) continue;     // could not write, so do not perform cleaning up
            try {
                deleteKeyFromCloud(provider.getId(), testKey);
            } catch (IOException e) {
                logger.warn("Could not remove {} from {}.", testKey, provider.getId());
            }
        }
    }    
    
    private int getAvailableProvidersCount() {
        int c = 0;
        for (CloudProvider cloud : providers.values())
            if (cloud.isEnabled()) c++;
        return c;
    }
    
    
    /**
     * TODO TEMP for dev purposes
     */
//    public static void main (String[] args){
//        KvStore kvs = new KvStore("hybrismytest");
//        kvs.put("mykey", "blablabla".getBytes());
//    }
}
