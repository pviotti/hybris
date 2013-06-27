package fr.eurecom.hybris.mds;

import java.io.Serializable;
import java.util.List;
import org.apache.commons.lang3.SerializationUtils;

/**
 * Timestamped directory class.
 * Holds the timestamped reference to the cloud replicas.
 * @author p.viotti
 */
public class Metadata implements Serializable {
    
    public static class Timestamp implements Serializable {

        private static final long serialVersionUID = -829397868153955069L;
        
        private int num;
        private String cid;
        
        public Timestamp(int n, String c) {
            this.num = n;
            this.cid = c;
        }

        public byte[] serialize() {
            return SerializationUtils.serialize(this);    
        }
        
        public int getNum()             { return num; }
        public void setNum(int num)     { this.num = num; }
        public String getCid()          { return cid; }
        public void setCid(String cid)  { this.cid = cid; }

        public boolean isGreater(Timestamp ts) {
            if ((this.num > ts.num) || 
                    ((this.num == ts.num) && (this.cid.compareTo(ts.cid)) < 0)) 
                return true;
            return false;   
        }
        
        public void inc(String client) {
            this.num = this.num + 1;
            this.cid = client;
        }
        
        @Override
        public String toString() {
            return this.num + "_" + this.cid;
        }

        @Override
        public boolean equals(Object obj) {
            Timestamp ts = (Timestamp) obj;
            return ( (this.cid.equals(ts.getCid())) && (this.num == ts.getNum()) );
        }
    }

    // TODO use protobuf instead of Java serialization
    private static final long serialVersionUID = -2127132184699014357L;

    private Timestamp ts;
    private byte[] hash;
    private List<String> replicasLst;
    
    public Metadata(Timestamp ts, byte[] hash, List<String> replicas) {
        this.ts = ts;
        this.hash = hash;
        this.replicasLst = replicas;
    }
    
    public Metadata(byte[] raw) {
        Metadata tsdir = (Metadata) SerializationUtils.deserialize(raw);
        this.ts = tsdir.ts;
        this.replicasLst = tsdir.replicasLst;
        this.hash = tsdir.getHash();
    }
    
    public byte[] serialize() {
        return SerializationUtils.serialize(this);    
    }
    
    public Timestamp getTs() { return ts; }
    public void setTs(Timestamp ts) { this.ts = ts;    }
    public List<String> getReplicasLst() { return replicasLst; }
    public void setReplicasLst(List<String> replicasLst) { this.replicasLst = replicasLst; }
    public byte[] getHash() { return hash; }
    public void setHash(byte[] hash) { this.hash = hash; }
    
    @Override
    public String toString() {
        StringBuilder strBld = new StringBuilder("TsDir [ts=" + ts + ", hash=" + hash + ", replicasLst={");
        if (replicasLst != null)
            for (String str : replicasLst)
                strBld.append(str + ",");
        strBld.append("}]");
        return strBld.toString();
    }
}
