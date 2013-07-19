package fr.eurecom.hybris.kvs;

import java.io.Serializable;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;

/**
 * Class for storing cloud storage provider details.
 * @author p.viotti
 */
public class CloudProvider implements Comparable<CloudProvider>, Serializable {

    private String id;

    private transient boolean enabled;
    private transient boolean alreadyUsed; /* whether the storage provider
                                            * has already been used
                                            * (to initialize the container */
    private transient BlobStore blobStore;

    /* measures to compare the providers */
    private transient long writeLatency;
    private transient long readLatency;
    private transient int cost; // $ cents per GB

    private static final long serialVersionUID = 1L;

    public CloudProvider(String id, String accessKey, String secretKey,
                            boolean enabled, int cost) {
        this.id = id;
        this.enabled = enabled;
        this.cost = cost;

        this.readLatency = 0;
        this.writeLatency = 0;
        this.alreadyUsed = false;

        BlobStoreContext context = ContextBuilder.newBuilder(id)
                                                .credentials(accessKey, secretKey)
                                                .buildView(BlobStoreContext.class);
        this.blobStore = context.getBlobStore();
    }

    public String getId()           { return this.id; }
    public void setId(String id)    { this.id = id; }
    public boolean isAlreadyUsed()  { return this.alreadyUsed; }
    public void setAlreadyUsed(boolean alreadyUsed) { this.alreadyUsed = alreadyUsed; }
    public boolean isEnabled()      { return this.enabled; }
    public void setEnabled(boolean enabled)         { this.enabled = enabled; }
    public long getWriteLatency()   { return this.writeLatency; }
    public void setWriteLatency(long writeLatency) { this.writeLatency = writeLatency; }
    public long getReadLatency()    { return this.readLatency; }
    public void setReadLatency(long readLatency)    { this.readLatency = readLatency; }
    public int getCost()            { return this.cost; }
    public void setCost(int cost)   { this.cost = cost; }
    public BlobStore getBlobStore() { return this.blobStore; }

    /*
     * TODO should they be comparable both in terms of cost and latency? if so,
     * which weight should be given to each of them
     */
    @Override
    public int compareTo(CloudProvider o) {
        if (this.writeLatency + this.readLatency < o.getWriteLatency()
                + o.getReadLatency()
                || !o.isEnabled())
            return -1;
        else if (this.writeLatency + this.readLatency > o.getWriteLatency()
                + o.getReadLatency()
                || !this.isEnabled())
            return 1;
        else
            return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.id == null ? 0 : this.id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (this.getClass() != obj.getClass())
            return false;
        CloudProvider other = (CloudProvider) obj;
        if (this.id == null) {
            if (other.id != null)
                return false;
        } else if (!this.id.equals(other.id))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return this.id;
    }

    public String toVerboseString() {
        return "CloudProvider (" + this.id + ") [alreadyUsed="
                + this.alreadyUsed + ", enabled=" + this.enabled
                + ", writeLatency=" + this.writeLatency + ", readLatency="
                + this.readLatency + ", cost=" + this.cost + "]";
    }
}