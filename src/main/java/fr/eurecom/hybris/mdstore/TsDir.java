package fr.eurecom.hybris.mdstore;

import java.io.Serializable;
import java.util.List;
import org.apache.commons.lang3.SerializationUtils;

/**
 * Timestamped directory class.
 * Holds the timestamped reference to the cloud replicas.
 * @author p.viotti
 */
public class TsDir implements Serializable {

	// TODO use protobuf instead of Java serialization
	private static final long serialVersionUID = -2127132184699014357L;

	private long ts;
	private String hash;
	private List<String> replicasLst;
	
	public TsDir(long ts, String hash, List<String> replicas) {
		this.ts = ts;
		this.hash = hash;
		this.replicasLst = replicas;
	}
	
	public TsDir(byte[] raw) {
		TsDir tsdir = (TsDir) SerializationUtils.deserialize(raw);
		this.ts = tsdir.ts;
		this.replicasLst = tsdir.replicasLst;
		this.hash = tsdir.getHash();
	}
	
	public byte[] serialize() {
		return SerializationUtils.serialize(this);	
	}
	
	public long getTs() { return ts; }
	public void setTs(long ts) { this.ts = ts;	}
	public List<String> getReplicasLst() { return replicasLst; }
	public void setReplicasLst(List<String> replicasLst) { this.replicasLst = replicasLst; }
	public String getHash() { return hash; }
	public void setHash(String hash) { this.hash = hash; }
	
	@Override
	public String toString() {
		StringBuilder strBld = new StringBuilder("TsDir [ts=" + ts + ", hash=" + hash + ", replicasLst={");
		for (String str : replicasLst)
			strBld.append(str + ",");
		strBld.append("}]");
		return strBld.toString();
	}
}
