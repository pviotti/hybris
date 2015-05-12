/**
 * Copyright (C) 2013 EURECOM (www.eurecom.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.eurecom.hybris.mds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.KvsManager.KvsId;
import fr.eurecom.hybris.kvs.drivers.Kvs;

/**
 * Holds timestamped metadata.
 * @author P. Viotti
 */
public class Metadata implements KryoSerializable {

    public static class Timestamp implements KryoSerializable {

        private int num;
        private String cid;

        public Timestamp() { }
        public Timestamp(int n, String c) {
            this.num = n;
            this.cid = c;
        }

        public static Timestamp parseString(String tsStr) {
            String[] tsParts = tsStr.split("_");
            int n = Integer.parseInt(tsParts[0]);
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

        public void write(Kryo kryo, Output output) {
            output.writeShort(this.num);
            output.writeAscii(this.cid);
        }
        public void read(Kryo kryo, Input input) {
            this.num = input.readShort();
            this.cid = input.readString();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    public enum MetadataType { TOMBSTONE, REPLICATION, EC };
    
    private Timestamp ts;
    private byte[] hash;
    private byte[][] chunksHashes;
    private byte[] cryptoKey;
    private int size;
    private List<Kvs> replicasLst;
    private MetadataType type;

    public Metadata() { }
    public Metadata(Timestamp ts, byte[] hash, int size,
            List<Kvs> replicas, byte[] cryptoKeyIV) {
        this.ts = ts;
        this.hash = hash;
        this.chunksHashes = null;
        this.size = size;
        this.replicasLst = replicas;
        this.cryptoKey = cryptoKeyIV;
        this.type = MetadataType.REPLICATION;
    }
    
    public Metadata(Timestamp ts, byte[][] hashes, 
            List<Kvs> replicas, int size, byte[] cryptoKeyIV) {
        this.ts = ts;
        this.hash = null;
        this.chunksHashes = hashes;
        this.size = size;
        this.replicasLst = replicas;
        this.cryptoKey = cryptoKeyIV;
        this.type = MetadataType.EC;
    }

    public Metadata(byte[] raw) {
        Kryo kryo = new Kryo();
        kryo.register(Metadata.class);
        kryo.register(Timestamp.class);
        Input input = new Input(raw);
        Metadata md = kryo.readObject(input, Metadata.class);
        input.close();
        this.ts = md.getTs();
        this.replicasLst = md.getReplicasLst();
        this.hash = md.getHash();
        this.chunksHashes = md.getChunksHashes();
        this.cryptoKey = md.getCryptoKey();
        this.size = md.getSize();
        this.type = md.getType();
    }
    
    public byte[] serialize() {
        Kryo kryo = new Kryo();
        kryo.register(Metadata.class);
        kryo.register(Timestamp.class);
        byte[] buff = new byte[512];
        Output output = new Output(buff);
        kryo.writeObject(output, this);
        output.close();
        return output.toBytes();
    }

    public static Metadata getTombstone(Timestamp ts) {
        Metadata md = new Metadata(ts, null, 0, null, null);
        md.setType(MetadataType.TOMBSTONE);
        return md;
    }
    
    public boolean isTombstone() {
        return this.hash == null &&
                this.chunksHashes == null &&
                this.replicasLst == null &&
                this.size == 0 &&
                this.cryptoKey == null;
    }

    public Timestamp getTs() { return this.ts; }
    public void setTs(Timestamp ts) { this.ts = ts;    }
    public List<Kvs> getReplicasLst() { return this.replicasLst; }
    public void setReplicasLst(List<Kvs> replicasLst) { this.replicasLst = replicasLst; }
    public byte[] getHash() { return this.hash; }
    public void setHash(byte[] hash) { this.hash = hash; }
    public byte[][] getChunksHashes() { return this.chunksHashes; }
    public void setChunksHashes(byte[][] hashes) { this.chunksHashes = hashes; }
    public int getSize() { return this.size; }
    public void setSize(int s) { this.size = s; }
    public byte[] getCryptoKey() { return this.cryptoKey; }
    public void setCryptoKey(byte[] ck) { this.cryptoKey = ck; }
    public MetadataType getType() { return this.type; }
    public void setType(MetadataType t) { this.type = t; }

    public String toString() {
        switch (type) {
            case TOMBSTONE:
                return "Metadata [Tombstone - ts=" + this.ts + "]";
            case REPLICATION:
                return "Metadata [Replicated - ts=" + this.ts + ", hash=" + Utils.bytesToHexStr(this.hash)
                        + ", size=" + this.size + ", replicasLst=" + this.replicasLst
                        + ", cryptoKey=" + Utils.bytesToHexStr(this.cryptoKey) + "]";
            case EC:
                return "Metadata [EC - ts=" + this.ts + ", size=" + this.size + 
                        ", replicasLst=" + this.replicasLst 
                        + ", cryptoKey=" + Utils.bytesToHexStr(this.cryptoKey) + "]";
            default:
                return "Metadata [(this_should_not_happen)]";
        }
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(this.cryptoKey);
        result = prime * result + Arrays.hashCode(this.hash);
        result = prime * result + Arrays.hashCode(this.chunksHashes);
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
        if (!Arrays.equals(this.cryptoKey, other.cryptoKey))
            return false;
        if (!Arrays.equals(this.hash, other.hash))
            return false;
        if (!Arrays.deepEquals(this.chunksHashes, other.chunksHashes))
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

    public void write(Kryo kryo, Output out) {
        kryo.writeClassAndObject(out, this.ts);
        
        switch (this.type) {
            case TOMBSTONE:
                out.writeByte(0x00);
                return;
            case REPLICATION:
                out.writeByte(0x01);
                out.write(this.hash);
                break;
            case EC:
                out.writeByte(0x02);
                out.writeByte(chunksHashes.length);
                for (byte[] h : chunksHashes)
                    out.write(h);
                break;
        }

        if (this.cryptoKey == null)
            out.writeByte(0x00);
        else {
            out.writeByte(0x01);
            out.write(this.cryptoKey);
        }

        out.writeInt(this.size);

        if (this.replicasLst != null)
            if (this.replicasLst.size() > 0)
                for (int i=0; i<this.replicasLst.size(); i++)
                    try {
                        out.writeByte( KvsId.valueOf( this.replicasLst.get(i).getId().toUpperCase() ).getSerial() );
                    } catch (IllegalArgumentException e) {
                        logger.error("Serialization of {} Kvs failed: Hybris could not find any suitable driver",
                                this.replicasLst.get(i).getId().toUpperCase());
                    }
            else
                out.writeByte(-1);     // empty replicas array
        else
            out.writeByte(-2);         // null replicas array
    }

    public void read(Kryo kryo, Input in) {
        this.ts = (Timestamp) kryo.readClassAndObject(in);
        
        switch (in.readByte()) {
            case 0x00:
                this.hash = null;
                this.chunksHashes = null;
                this.replicasLst = null;
                this.size = 0;
                this.cryptoKey = null;
                this.type = MetadataType.TOMBSTONE;
                return;
            case 0x01:
                this.chunksHashes = null;
                this.hash = in.readBytes(Utils.HASH_LENGTH);
                this.type = MetadataType.REPLICATION;
                break;
            case 0x02:
                this.hash = null;
                short len = (short) in.readByte();
                this.chunksHashes = new byte[len][];
                for (short i=0; i<len; i++)
                    this.chunksHashes[i] = in.readBytes(Utils.HASH_LENGTH);
                this.type = MetadataType.EC;
                break;
        }

        switch (in.readByte()) {
            case 0x00:
                this.cryptoKey = null;
                break;
            case 0x01:
                this.cryptoKey = in.readBytes(Utils.CRYPTO_KEY_LENGTH);
                break;
        }

        this.size = in.readInt();

        this.replicasLst = new ArrayList<Kvs>();
        while (true) {
            short rep;
            try {
                rep = in.readByte();
            } catch(Exception e) {
                break;
            }
            if (rep == -1)          // empty replicas array
                break;
            else if (rep == -2) {   // null replicas array
                this.replicasLst = null;
                break;
            }

            try {
                switch (KvsId.getIdFromSerial(rep)) {
                    case AMAZON:
                        this.replicasLst.add(new Kvs(KvsId.AMAZON.toString(), null, false, 0));
                        break;
                    case AZURE:
                        this.replicasLst.add(new Kvs(KvsId.AZURE.toString(), null, false, 0));
                        break;
                    case GOOGLE:
                        this.replicasLst.add(new Kvs(KvsId.GOOGLE.toString(), null, false, 0));
                        break;
                    case RACKSPACE:
                        this.replicasLst.add(new Kvs(KvsId.RACKSPACE.toString(), null, false, 0));
                        break;
                    case TRANSIENT:
                        this.replicasLst.add(new Kvs(KvsId.TRANSIENT.toString(), null, false, 0));
                        break;
                    case FAULTY:
                        this.replicasLst.add(new Kvs(KvsId.FAULTY.toString(), null, false, 0));
                        break;
                    default:
                        break;
                }
            } catch (IllegalArgumentException e) {
                logger.error("Deserialization of {} Kvs failed: Hybris could not find any suitable driver", rep);
            }
        }
    }
}
