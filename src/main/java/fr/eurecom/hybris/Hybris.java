package fr.eurecom.hybris;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import fr.eurecom.hybris.kvs.KvStore;
import fr.eurecom.hybris.mdstore.MdStore;
import fr.eurecom.hybris.mdstore.TsDir;

public class Hybris implements HybrisInterface {

	String clientName = "marko";		//FIXME clientName
	
	private MdStore mds;
	private KvStore kvs;
	
	public Hybris() {
		mds = new MdStore("127.0.0.1:2181","/MdStore");
		kvs = new KvStore();
	}
	
	
	// TODO move elsewhere
	private byte[] getHash(byte[] inputBytes) {
		MessageDigest hash;
		try {
			hash = MessageDigest.getInstance("SHA-1");
			hash.reset();
		    hash.update(inputBytes);
		    return hash.digest();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	private boolean check(TsDir tsdir, byte[] v) {
		if (getHash(v).equals(mds.read(tsdir)))
			return true;
		return false;
	}
	

	public byte[] read(String key) {
		
		//TsDir tsdir = mds.tsRead(key);
		
		byte[] ret = null;
		
		/*if (tsdir == null)
			return null;
		for (String cloud : tsdir.getReplicasLst()) {
			//TODO read from cloud
		}*/	
		
		return ret;
	}

	public void write(String key, byte[] value) {
		
		//TsDir tsdir = mds.tsRead(key);
		
		
		// FIXME initialization of the Znode
		//tsdir.getTs().inc(clientName);
		
		mds.write(new TsDir("test".getBytes()), getHash(value));
		
		//TODO integrate with clouds
		
		List<String> datalist = Arrays.asList(new String[] {"Azure","S3"});
		
		// End TODO
		
		//tsdir.setReplicasLst(datalist);
		//mds.tsWrite(tsdir);
	}
	
	public void gc() {
		// TODO Auto-generated method stub
	}
	
	
	/**
	 * TODO TEMP for debugging purposes
	 */
	public static void main(String[] args) {
		
		Hybris hybris = new Hybris();
		hybris.write("/path/to/file/filename.dat:chunk001", "my_value".getBytes());
		String value = new String(hybris.read("key"));
		System.out.println("Read output: " + value);
	}
}
