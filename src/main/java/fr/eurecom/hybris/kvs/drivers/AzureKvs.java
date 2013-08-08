package fr.eurecom.hybris.kvs.drivers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.windowsazure.services.blob.client.CloudBlob;
import com.microsoft.windowsazure.services.blob.client.CloudBlobClient;
import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
import com.microsoft.windowsazure.services.blob.client.CloudBlockBlob;
import com.microsoft.windowsazure.services.blob.client.ListBlobItem;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;

import fr.eurecom.hybris.Config;

public class AzureKvs extends Kvs {

    private static final long serialVersionUID = 1L;
    private transient static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private static final String ERROR_BLOB_NOT_FOUND = "BlobNotFound";

    private transient final CloudBlobClient blobClient;
    private transient CloudBlobContainer containerRef;
    private transient HashMap<String, CloudBlockBlob> blobRefs;     // cached references of already used blobs

    public AzureKvs(String id, String accessKey, String secretKey,
                        String container, boolean enabled, int cost) throws IOException {
        super(id, accessKey, secretKey, container, enabled, cost);

        String storageConnectionString = "DefaultEndpointsProtocol=http;" +
                                            "AccountName=" + accessKey +
                                            ";AccountKey=" + secretKey;
        CloudStorageAccount storageAccount;
        try {
            storageAccount = CloudStorageAccount.parse(storageConnectionString);
            this.blobClient = storageAccount.createCloudBlobClient();
            this.blobClient.setSingleBlobPutThresholdInBytes(30000000);  // 30 MB
            this.blobClient.setConcurrentRequestCount(4);
        } catch (InvalidKeyException | URISyntaxException e) {
            logger.error("Could not initialize {} KvStore", id, e);
            throw new IOException(e);
        }

        this.createContainer();
        this.blobRefs = new HashMap<String, CloudBlockBlob>();
    }

    public void put(String key, byte[] value) throws IOException {
        try {
            CloudBlockBlob blob = this.containerRef.getBlockBlobReference(key);
            blob.getProperties().setContentMD5(null);
            blob.upload(new ByteArrayInputStream(value), value.length);
            this.blobRefs.put(key, blob);
        } catch (URISyntaxException | StorageException | IOException e) {
            throw new IOException(e);
        }
    }

    public byte[] get(String key) throws IOException {
        try {
            CloudBlockBlob blob = this.blobRefs.get(key);
            if (blob == null)
                blob = this.containerRef.getBlockBlobReference(key);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            blob.download(baos);
            return baos.toByteArray();
        } catch (URISyntaxException | StorageException | IOException e) {

            if (e instanceof StorageException) {
                StorageException se = (StorageException) e;
                if (ERROR_BLOB_NOT_FOUND.equals(se.getErrorCode()))
                    return null;
            }

            throw new IOException(e);
        }
    }

    public void delete(String key) throws IOException {
        try {
            CloudBlockBlob blob = this.blobRefs.get(key);
            if (blob == null)
                blob = this.containerRef.getBlockBlobReference(key);
            blob.delete();
            this.blobRefs.remove(key);
        } catch (URISyntaxException | StorageException e) {

            if (e instanceof StorageException) {
                StorageException se = (StorageException) e;
                if (ERROR_BLOB_NOT_FOUND.equals(se.getErrorCode()))
                    return;
            }

            throw new IOException(e);
        }
    }

    public List<String> list() throws IOException {
        try {
            List<String> keys = new ArrayList<String>();
            for (ListBlobItem blobItem : this.containerRef.listBlobs()) { // TODO useFlatBlobListing flag to get only blobs regardless of virtual directories
                CloudBlob blob = (CloudBlob) blobItem;
                keys.add(blob.getName());
            }
            return keys;
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    protected void createContainer() throws IOException {
        try {
            this.containerRef = this.blobClient.getContainerReference(this.rootContainer);
            this.containerRef.createIfNotExist();
        } catch (StorageException | URISyntaxException e) {
            throw new IOException(e);
        }
    }

    public void shutdown() throws IOException { }
}
