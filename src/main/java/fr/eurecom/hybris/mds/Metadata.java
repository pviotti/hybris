package fr.eurecom.hybris.mds;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.SerializationUtils;

import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.CloudProvider;

/**
 * Class representing timestamped metadata.
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
        
        public static Timestamp parseString(String tsStr) {
            String[] tsParts = tsStr.split("_");
            int n =Integer.parseInt(tsParts[0]);
            String cid = tsParts[1];
            return new Timestamp(n, cid);            
        }
        
        public int getNum()             { return num; }
        public void setNum(int num)     { this.num = num; }
        public String getCid()          { return cid; }
        public void setCid(String cid)  { this.cid = cid; }

        public boolean isGreater(Timestamp ts) {
            if ((ts == null) || (this.num > ts.num) || 
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
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((cid == null) ? 0 : cid.hashCode());
            result = prime * result + num;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Timestamp other = (Timestamp) obj;
            if (cid == null) {
                if (other.cid != null)
                    return false;
            } else if (!cid.equals(other.cid))
                return false;
            if (num != other.num)
                return false;
            return true;
        }
    }

    // TODO use protobuf instead of Java serialization
    private static final long serialVersionUID = -2127132184699014357L;

    private Timestamp ts;
    private byte[] hash;
    private List<CloudProvider> replicasLst;
    
    public Metadata(Timestamp ts, byte[] hash, List<CloudProvider> replicas) {
        this.ts = ts;
        this.hash = hash;
        this.replicasLst = replicas;
    }
    
    public Metadata(byte[] raw) {
        Metadata md;
        if (raw == null)
            md = new Metadata(null, null, null);
        else
            md = (Metadata) SerializationUtils.deserialize(raw);
        this.ts = md.ts;
        this.replicasLst = md.replicasLst;
        this.hash = md.getHash();
    }
    
    public byte[] serialize() {
        return SerializationUtils.serialize(this);    
    }
    
    public boolean isTombstone() {
        return ((this.hash == null) && 
                (this.replicasLst == null) && 
                (this.ts == null));
    }
    
    public Timestamp getTs() { return ts; }
    public void setTs(Timestamp ts) { this.ts = ts;    }
    public List<CloudProvider> getReplicasLst() { return replicasLst; }
    public void setReplicasLst(List<CloudProvider> replicasLst) { this.replicasLst = replicasLst; }
    public byte[] getHash() { return hash; }
    public void setHash(byte[] hash) { this.hash = hash; }
    
    @Override
    public String toString() {
        StringBuilder strBld = new StringBuilder("Timestamp [ts=" + ts + ", "
                                                    + "hash=" + Utils.byteArrayToHexString(hash) + ", replicasLst={");
        if (replicasLst != null)
            for (CloudProvider replica : replicasLst)
                strBld.append(replica.getId() + ",");
        strBld.append("}]");
        return strBld.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(hash);
        result = prime * result
                + ((replicasLst == null) ? 0 : replicasLst.hashCode());
        result = prime * result + ((ts == null) ? 0 : ts.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Metadata other = (Metadata) obj;
        if (!Arrays.equals(hash, other.hash))
            return false;
        if (replicasLst == null) {
            if (other.replicasLst != null)
                return false;
        } else if (!replicasLst.equals(other.replicasLst))
            return false;
        if (ts == null) {
            if (other.ts != null)
                return false;
        } else if (!ts.equals(other.ts))
            return false;
        return true;
    }
}
