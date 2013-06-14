package fr.eurecom.hybris.mdstore;

import java.io.Serializable;
import java.util.List;
import org.apache.commons.lang3.SerializationUtils;


public class TsDir implements Serializable {
	
	public class Timestamp implements Serializable {

		private static final long serialVersionUID = -829397868153955069L;
		private int num;
		private String cid;
		
		public Timestamp(int n, String c) {
			this.num = n;
			this.cid = c;
		}
		
		public int getNum() { return num; }
		public void setNum(int num) { this.num = num; }
		public String getCid() { return cid; }
		public void setCid(String cid) { this.cid = cid; }
		
		public boolean isGreater(Timestamp ts) {
			if ((this.num > ts.num) || ((this.num == ts.num) && (this.cid.compareTo(ts.cid)) > 0)) 
				return true;
			return false;	
		}
		
		public void inc(String client) {
			this.num = this.num + 1;
			this.cid = client;
		}
	}
	
	private static final long serialVersionUID = -2127132184699014357L;
	private Timestamp ts;
	private String hash;
	private List<String> replicasLst;
	

	public TsDir(byte[] raw) {
		TsDir tsdir = (TsDir) SerializationUtils.deserialize(raw);
		this.ts = tsdir.ts;
		this.replicasLst = tsdir.replicasLst;
	}
	
	public Timestamp getTs() { return ts; }
	public void setTs(Timestamp ts) { this.ts = ts;	}
	public List<String> getReplicasLst() { return replicasLst; }
	public void setReplicasLst(List<String> replicasLst) { this.replicasLst = replicasLst; }
	public String getHash() { return hash; }
	public void setHash(String hash) { this.hash = hash; }
	
	public TsDir(Timestamp ts, List<String> replicas) {
		this.ts = ts;
		this.replicasLst = replicas;
	}
	
	public TsDir(int num, String cid, List<String> dl) {
		Timestamp ts = new Timestamp(num,cid);
		this.ts = ts;
		this.replicasLst = dl;
	}
	
	public byte[] serialize() {
		return SerializationUtils.serialize(this);	
	}
}
