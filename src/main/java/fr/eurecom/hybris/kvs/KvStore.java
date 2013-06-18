package fr.eurecom.hybris.kvs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;

import com.google.common.io.ByteStreams;

import fr.eurecom.hybris.Config;

/**
 * KvStore exposes a simple common cloud storage API
 * for saving, retrieving and deleting data
 * on the supported cloud storage services.
 * @author p.viotti
 */
public class KvStore {
    
    private static Logger logger = Logger.getLogger(Config.LOGGER_NAME);
    
    private Map<String, CloudProvider> providers;
    private List<CloudProvider> sortedProviders;      // storage providers sorted by cost and latency
    
    private String rootContainer;

    
    public KvStore(String rootContainer) {
        
        this.rootContainer = rootContainer;
        
        // TODO retrieve them from a configuration file
        providers = Collections.synchronizedMap(new HashMap<String, CloudProvider>());
        providers.put("aws-s3", new CloudProvider("aws-s3", System.getenv("s3user"), 
                                                    System.getenv("s3pass"), true, 0));
        providers.put("cloudfiles-us", new CloudProvider("cloudfiles-us", System.getenv("rackspaceuser"), 
                                                           System.getenv("rackspacepass"), true, 0));
        providers.put("azureblob", new CloudProvider("azureblob", System.getenv("azureuser"), 
                                                       System.getenv("azurepass"), true, 0));
        //providers.put("hpcloud-objectstorage", new StorageProvider("hpcloud-objectstorage", System.getenv("hpclouduser"), 
                                           //System.getenv("hpcloudpass"), false, 0, 0));
        
        // Sort providers according to cost and latency
        sortedProviders = new ArrayList<CloudProvider>(providers.values());
        performLatencyTests();
        Collections.sort(sortedProviders);
        for(CloudProvider cloud : sortedProviders) System.out.println(cloud); // TODO TEMP
    }
    
    // =======================================================================================
    //                                      PUBLIC APIs
    // ---------------------------------------------------------------------------------------
    
    public List<String> put(String key, byte[] data) {
        
        List<String> successSavedKvsLst = new ArrayList<String>();
        
        synchronized (providers) {
            for (CloudProvider provider : sortedProviders) {
                try {
                    putInCloud(provider, key, data);                    
                    successSavedKvsLst.add(provider.getName());
                } catch (Exception e) {
                    logger.error("error while storing " + key + " on " + provider.getName(), e);
                }
            }
        }
        return successSavedKvsLst;
    }
    
    public byte[] getFromCloud(String provider, String key) {
        
        BlobStoreContext context = null; 
        BlobStore storage = null; 
        Blob blob = null;
        
        CloudProvider cloud = providers.get(provider);
        try {
            context = ContextBuilder.newBuilder(cloud.getName())
                                    .credentials(cloud.getAccountName(), cloud.getAccessKey())
                                    .buildView(BlobStoreContext.class);
            storage = context.getBlobStore();
            blob = storage.getBlob(rootContainer, key);
            // TODO check whether it throws an exception or return null in case of key not found
            return ByteStreams.toByteArray(blob.getPayload());
        } catch (Exception e) {
            logger.error("error while retrieving " + key + " on " + cloud.getName(), e);
            return null;
        } finally {
            if (context != null)
                context.close();
        }
    }
    
    public void deleteKeyFromCloud(String provider, String key) {
        
        BlobStoreContext context = null; 
        BlobStore storage = null; 
        
        CloudProvider cloud = providers.get(provider);
        try {
            context = ContextBuilder.newBuilder(cloud.getName())
                                    .credentials(cloud.getAccountName(),
                                                 cloud.getAccessKey())
                                    .buildView(BlobStoreContext.class);

            storage = context.getBlobStore();
            storage.removeBlob(rootContainer, key);

        } catch (Exception ex) {
            logger.error("error while deleting " + key + " from " + provider);
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
        
        context = ContextBuilder.newBuilder(provider.getName())
                                .credentials(provider.getAccountName(), provider.getAccessKey())
                                .buildView(BlobStoreContext.class);
        storage = context.getBlobStore();
        
        if (!provider.isAlreadyUsed()) {
            storage.createContainerInLocation(null, rootContainer);
            provider.setAlreadyUsed(true);
            logger.debug("created container " + rootContainer + " for provider " + provider.getName());
        }
        
        blob = storage.blobBuilder(key).payload(data).build();
        storage.putBlob(rootContainer, blob);
    }
    
//    private void deleteContainerFromCloud(String provider, String container) {
//        // TODO
//    }
    
    private void performLatencyTests() {
        
        byte[] testData = "1234567890QWERTYUIOPASDFGHJKLZXCVBNM".getBytes();
        String testKey = "latency_test-" + (new Random()).nextInt(1000);
        long start, end = 0;
        
        // Perform write tests
        for (CloudProvider provider : sortedProviders) {
            try {
                start = System.currentTimeMillis();
                putInCloud(provider, testKey, testData);
                end = System.currentTimeMillis();
                provider.setWriteLatency(end - start);
            } catch (Exception e) {
                logger.error("error while storing " + testKey + " on " + provider.getName(), e);
                provider.setWriteLatency(-1);
            }
        }
        
        // Perform read tests
        for (CloudProvider provider : sortedProviders) {
            if (provider.getWriteLatency() == -1) continue;     // could not write, so do not perform reading test
            try {
                start = System.currentTimeMillis();
                byte[] retrieved = getFromCloud(provider.getName(), testKey);
                end = System.currentTimeMillis();
                if (!Arrays.equals(testData, retrieved)) {
                    logger.warn("retrieved blob does not match original data: " + new String(retrieved));
                    provider.setReadLatency(-1);
                } else
                    provider.setReadLatency(end - start);
            } catch (Exception e) {
                logger.error("error while retrieving " + testKey + " on " + provider.getName(), e);
                provider.setReadLatency(-1);
            }
        }
        
        // Clean up test data
        for (CloudProvider provider : sortedProviders) {
            if (provider.getWriteLatency() == -1) continue;     // could not write, so do not perform cleaning up
            deleteKeyFromCloud(provider.getName(), testKey);
        }
    }    
    
    /**
     * TODO TEMP for dev purposes
     */
    public static void main (String[] args){
        Config.getInstance();
        KvStore kvs = new KvStore("hybrismytest");
//        kvs.put("mykey", "blablabla".getBytes());
    }
}
