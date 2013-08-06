package fr.eurecom.hybris.kvs.drivers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStores;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

import fr.eurecom.hybris.Config;

public class RackspaceKvs extends Kvs {

    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private final static String rackspaceId = "cloudfiles-us";
    private transient final BlobStore blobStore;

    public RackspaceKvs(String id, String accessKey, String secretKey,
                            String container, boolean enabled, int cost) {
        super(id, accessKey, secretKey, container, enabled, cost);
        BlobStoreContext context = ContextBuilder.newBuilder(rackspaceId)
                                                .credentials(accessKey, secretKey)
                                                .buildView(BlobStoreContext.class);
        this.blobStore = context.getBlobStore();
    }

    public void put(String key, byte[] value) throws IOException {
        try {
            Blob blob = this.blobStore.blobBuilder(key).payload(value).build();
            this.blobStore.putBlob(this.rootContainer, blob);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public byte[] get(String key) throws IOException {
        try {
            Blob blob = this.blobStore.getBlob(this.rootContainer, key);
            if (blob == null) {
                logger.warn("Could not find key {} in {}", key, this.id);
                return null;
            }
            return ByteStreams.toByteArray(blob.getPayload());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void delete(String key) throws IOException {
        try {
            this.blobStore.removeBlob(this.rootContainer, key);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public List<String> list() throws IOException {
        try {
            List<String> keys = new ArrayList<String>();
            for (StorageMetadata resourceMd :
                    BlobStores.listAll(this.blobStore,
                                        this.rootContainer,
                                        ListContainerOptions.NONE))
                keys.add(resourceMd.getName());
            return keys;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public void createContainer() throws IOException {
        try {
            this.blobStore.createContainerInLocation(null, this.rootContainer);
            this.alreadyUsed = true;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
