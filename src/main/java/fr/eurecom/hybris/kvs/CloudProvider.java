package fr.eurecom.hybris.kvs;

/**
 * Class for storing cloud storage provider details.
 * @author p.viotti
 */
public class CloudProvider implements Comparable<CloudProvider> {
    
    private String name;
    private String accountName;
    private String accessKey;
    
    private boolean alreadyUsed;   // whether the storage provider has already been used (to initialize the folder/bucket)
    private boolean enabled;

    /* measures to compare the providers */
    private long writeLatency;
    private long readLatency;
    private int cost;          // $ cents per GB

    public CloudProvider(String name, String accountName, String accessKey,
            boolean enabled, int cost) {
        super();
        this.name = name;
        this.accountName = accountName;
        this.accessKey = accessKey;
        this.enabled = enabled;
        this.cost = cost;
        
        this.readLatency = 0;
        this.writeLatency = 0;
        this.alreadyUsed = false;
    }
    
    public String getName()             { return name; }
    public void setName(String name)     { this.name = name; }
    public String getAccountName()                     { return accountName; }
    public void setAccountName(String accountName)     { this.accountName = accountName; }
    public String getAccessKey()                 { return accessKey; }
    public void setAccessKey(String accessKey)     {this.accessKey = accessKey;    }
    public boolean isAlreadyUsed()                     { return alreadyUsed; }
    public void setAlreadyUsed(boolean alreadyUsed) { this.alreadyUsed = alreadyUsed; }
    public long getWriteLatency()                     { return writeLatency; }
    public void setWriteLatency(long writeLatency)     { this.writeLatency = writeLatency; }
    public int getCost()                   { return cost; }
    public void setCost(int cost)          { this.cost = cost; }
    public long getReadLatency()                    { return readLatency; }
    public void setReadLatency(long readLatency)    { this.readLatency = readLatency; }
    public boolean isEnabled()                      { return enabled; }
    public void setEnabled(boolean enabled)         { this.enabled = enabled; }
    
    /* 
     * TODO should they be comparable both in terms of cost and latency?
     * if so, which weight should be given to each of them
     */
    @Override
    public int compareTo(CloudProvider o) {
        if     (((this.writeLatency + this.readLatency) < 
                (o.getWriteLatency() + o.getReadLatency()))    
                || (!o.isEnabled()))    
            return -1;
        else if (((this.writeLatency + this.readLatency) > 
                    (o.getWriteLatency() + o.getReadLatency())) 
                    || (!this.isEnabled()))
            return 1;
        else
            return 0;
    }
    
    @Override
    public String toString() {
        return "CloudProvider [name=" + name + ", alreadyUsed=" + alreadyUsed
                + ", enabled=" + enabled + ", writeLatency=" + writeLatency
                + ", readLatency=" + readLatency + ", cost=" + cost + "]";
    }
}
