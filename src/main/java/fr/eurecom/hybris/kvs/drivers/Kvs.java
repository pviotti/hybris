package fr.eurecom.hybris.kvs.drivers;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public abstract class Kvs implements Comparable<Kvs>, Serializable {

    protected final String id;
    protected transient boolean enabled;
    protected transient String rootContainer;

    /* measures to compare the providers */
    protected transient long writeLatency;
    protected transient long readLatency;
    protected transient int cost; // $ cents per GB

    protected static final long serialVersionUID = 1L;

    public Kvs(String id, String accessKey, String secretKey,
                        String container, boolean enabled, int cost) {
        this.id = id;
        this.enabled = enabled;
        this.cost = cost;
        this.rootContainer = container;

        this.readLatency = 0;
        this.writeLatency = 0;
    }

    public String getId()           { return this.id; }
    public boolean isEnabled()      { return this.enabled; }
    public void setEnabled(boolean enabled)         { this.enabled = enabled; }
    public long getWriteLatency()   { return this.writeLatency; }
    public void setWriteLatency(long writeLatency) { this.writeLatency = writeLatency; }
    public long getReadLatency()    { return this.readLatency; }
    public void setReadLatency(long readLatency)    { this.readLatency = readLatency; }
    public int getCost()            { return this.cost; }
    public void setCost(int cost)   { this.cost = cost; }

    /* APIs */
    public abstract void put(String key, byte[] value) throws IOException;
    public abstract byte[] get(String key) throws IOException;
    public abstract List<String> list() throws IOException;
    public abstract void delete(String key) throws IOException;
    protected abstract void createContainer() throws IOException;

    /*
     * TODO should they be comparable both in terms of cost and latency? if so,
     * which weight should be given to each of them
     */
    @Override
    public int compareTo(Kvs o) {
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
        Kvs other = (Kvs) obj;
        if (this.id == null) {
            if (other.id != null)
                return false;
        } else if (!this.id.equals(other.id))
            return false;
        return true;
    }

    public String toString() {
        return this.id;
    }

    public String toVerboseString() {
        return "Kvs (" + this.id + ") [enabled=" + this.enabled +
                ", writeLatency=" + this.writeLatency +
                ", readLatency=" + this.readLatency +
                ", cost=" + this.cost + "]";
    }
}
