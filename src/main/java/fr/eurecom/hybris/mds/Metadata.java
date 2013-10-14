package fr.eurecom.hybris.mds;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.SerializationUtils;

import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.kvs.drivers.TransientKvs;

/**
 * Holds timestamped metadata.
 * @author P. Viotti
 * 
 * TODO try to use Protobuf, Thrift, Avro, or Cap'n Proto instead of Java serialization
 */
public class Metadata implements Externalizable, Serializable {

    public static class Timestamp implements Externalizable, Serializable {

        private int num;
        private String cid;

        private static final long serialVersionUID = 6529685098267757690L;

        public Timestamp() { }
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

        public int getNum()             { return this.num; }
        public void setNum(int num)     { this.num = num; }
        public String getCid()          { return this.cid; }
        public void setCid(String cid)  { this.cid = cid; }

        public boolean isGreater(Timestamp ts) {
            if (ts == null || this.num > ts.num ||
                    this.num == ts.num && this.cid.compareTo(ts.cid) < 0)
                return true;
            return false;
        }

        public void inc(String client) {
            this.num = this.num + 1;
            this.cid = client;
        }

        public String toString() {
            return this.num + "_" + this.cid;
        }

        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (this.cid == null ? 0 : this.cid.hashCode());
            result = prime * result + this.num;
            return result;
        }

        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (this.getClass() != obj.getClass())
                return false;
            Timestamp other = (Timestamp) obj;
            if (this.cid == null) {
                if (other.cid != null)
                    return false;
            } else if (!this.cid.equals(other.cid))
                return false;
            if (this.num != other.num)
                return false;
            return true;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.num = in.readInt();
            this.cid = in.readUTF();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(this.num);
            out.writeUTF(this.cid);
        }
    }

    private Timestamp ts;
    private byte[] hash;
    private int size;
    private List<Kvs> replicasLst;

    private static final long serialVersionUID = 6529685098267747690L;

    public Metadata() { }
    public Metadata(Timestamp ts, byte[] hash, int size, List<Kvs> replicas) {
        this.ts = ts;
        this.hash = hash;
        this.size = size;
        this.replicasLst = replicas;
    }

    public Metadata(byte[] raw) {
        Metadata md;
        if (raw == null)
            md = new Metadata(null, null, 0, null);
        else
            md = (Metadata) SerializationUtils.deserialize(raw);
        this.ts = md.ts;
        this.replicasLst = md.replicasLst;
        this.hash = md.getHash();
        this.size = md.getSize();
    }

    public static Metadata getTombstone() {
        return new Metadata(null, null, 0, null);
    }

    public byte[] serialize() {
        return SerializationUtils.serialize(this);
    }

    public boolean isTombstone() {
        return this.hash == null &&
                this.replicasLst == null &&
                this.ts == null;
    }

    public Timestamp getTs() { return this.ts; }
    public void setTs(Timestamp ts) { this.ts = ts;    }
    public List<Kvs> getReplicasLst() { return this.replicasLst; }
    public void setReplicasLst(List<Kvs> replicasLst) { this.replicasLst = replicasLst; }
    public byte[] getHash() { return this.hash; }
    public void setHash(byte[] hash) { this.hash = hash; }
    public int getSize() { return this.size; }
    public void setSize(int s) { this.size = s; }

    public String toString() {
        return "Metadata [ts=" + this.ts + ", hash=" + Utils.bytesToHexStr(this.hash)
                + ", size=" + this.size + ", replicasLst=" + this.replicasLst + "]";
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(this.hash);
        result = prime * result
                + (this.replicasLst == null ? 0 : this.replicasLst.hashCode());
        result = prime * result + this.size;
        result = prime * result + (this.ts == null ? 0 : this.ts.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (this.getClass() != obj.getClass())
            return false;
        Metadata other = (Metadata) obj;
        if (!Arrays.equals(this.hash, other.hash))
            return false;
        if (this.replicasLst == null) {
            if (other.replicasLst != null)
                return false;
        } else if (!this.replicasLst.equals(other.replicasLst))
            return false;
        if (this.size != other.size)
            return false;
        if (this.ts == null) {
            if (other.ts != null)
                return false;
        } else if (!this.ts.equals(other.ts))
            return false;
        return true;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.ts = (Timestamp) in.readObject();
        this.hash = new byte[Utils.HASH_LENGTH];
        in.readFully(this.hash, 0, Utils.HASH_LENGTH);
        byte[] ba = new byte[Utils.HASH_LENGTH];
        Arrays.fill(ba, (byte) 0x0);
        if (Arrays.equals(ba, this.hash))
            this.hash = null;
        this.size = in.readInt();

        String replicasStr = in.readUTF();
        if (replicasStr.equalsIgnoreCase(""))       // null
            this.replicasLst = null;
        else if (replicasStr.equalsIgnoreCase("*")) // empty array
            this.replicasLst = new ArrayList<Kvs>();
        else {
            List<Kvs> replicas = new ArrayList<Kvs>();
            String[] reps = replicasStr.split(",");
            for (String repIds : reps)
                replicas.add(new TransientKvs(repIds, null, null, null, false, 0));
            this.replicasLst = replicas;
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.ts);
        if (this.hash == null){
            byte[] ba = new byte[Utils.HASH_LENGTH];
            Arrays.fill(ba, (byte) 0x0);
            out.write(ba);
        } else
            out.write(this.hash, 0, this.hash.length);
        out.writeInt(this.size);

        StringBuilder sb = new StringBuilder();
        if (this.replicasLst != null)
            if (this.replicasLst.size() > 0)
                for (int i=0; i<this.replicasLst.size(); i++) {
                    sb.append(this.replicasLst.get(i).getId());
                    if (i != this.replicasLst.size()-1)
                        sb.append(",");
                }
            else
                sb.append("*"); // encode empty replica array
        else
            sb.append("");      // encode null value
        out.writeUTF(sb.toString());
    }
}
