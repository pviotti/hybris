package fr.eurecom.hybris.kvs.drivers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
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

    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private final CloudBlobClient blobClient;

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
        } catch (InvalidKeyException | URISyntaxException e) {
            logger.error("Could not initialize {} KvStore", id, e);
            throw new IOException(e);
        }
    }

    public void put(String key, byte[] value) throws IOException {
        try {
            CloudBlobContainer container = this.blobClient.getContainerReference(this.rootContainer);
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            blob.upload(new ByteArrayInputStream(value), value.length);
        } catch (URISyntaxException | StorageException | IOException e) {
            logger.warn("Could not put {}", key, e);
            throw new IOException(e);
        }
    }

    public byte[] get(String key) throws IOException {
        try {
            CloudBlobContainer container = this.blobClient.getContainerReference(this.rootContainer);
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            blob.download(baos);
            return baos.toByteArray();
        } catch (URISyntaxException | StorageException | IOException e) {
            logger.warn("Could not get {}", key, e);
            throw new IOException(e);
        }
    }

    public void delete(String key) throws IOException {
        try {
            CloudBlobContainer container = this.blobClient.getContainerReference(this.rootContainer);
            CloudBlockBlob blob = container.getBlockBlobReference(key);
            blob.delete();
        } catch (URISyntaxException | StorageException e) {
            logger.warn("Could not delete {}", key, e);
            throw new IOException(e);
        }
    }

    public List<String> list() throws IOException {
        try {
            List<String> keys = new ArrayList<String>();
            CloudBlobContainer container = this.blobClient.getContainerReference(this.rootContainer);
            for (ListBlobItem blobItem : container.listBlobs()) { // TODO useFlatBlobListing flag to get only blobs regardless of virtual directories
                CloudBlob blob = (CloudBlob) blobItem;
                keys.add(blob.getName());
            }
            return keys;
        } catch (URISyntaxException | StorageException e) {
            logger.warn("Could not list {}", this.rootContainer, e);
            throw new IOException(e);
        }
    }

    public boolean createContainer() throws IOException {
        try {
            CloudBlobContainer container = this.blobClient.getContainerReference(this.rootContainer);
            return container.createIfNotExist();
        } catch (StorageException | URISyntaxException e) {
            logger.warn("Could not create " + this.rootContainer + " on " + this.id, e);
            throw new IOException(e);
        }
    }
}
