package fr.eurecom.hybris;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import fr.eurecom.hybris.kvs.KvStore;
import fr.eurecom.hybris.mdstore.MdStore;
import fr.eurecom.hybris.mdstore.TsDir;

public class Hybris implements HybrisInterface {

	private MdStore mds;
	private KvStore kvs;
	
	private Config conf = Config.getInstance();
	
	public Hybris() throws IOException {
		mds = new MdStore(conf.getProperty(Config.ZK_ADDR), 
							conf.getProperty(Config.ZK_ROOT));
//		kvs = new KvStore();
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
			e.printStackTrace();
		}
		return null;
	}
	
	private boolean check(TsDir tsdir, byte[] v) {
		// TODO
//		if (getHash(v).equals(mds.read(tsdir)))
//			return true;
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
		
//		TsDir tsdir = mds.tsRead(key);
		
		
		// FIXME initialization of the Znode
		//tsdir.getTs().inc(clientName);
		
//		mds.write(new TsDir("test".getBytes()), getHash(value));
		
		//TODO integrate with clouds
		
		List<String> datalist = Arrays.asList(new String[] {"Azure","S3"});
		
		// End TODO
		
			//tsdir.setReplicasLst(datalist);
		//mds.tsWrite(tsdir);
	}
	
	public void gc() {
		// TODO
	}
	
	
	/**
	 * TODO TEMP for debugging purposes
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Hybris hybris = new Hybris();
		hybris.write("/bucketxyz/filename.dat", "my_value".getBytes());
		String value = new String(hybris.read("key"));
		System.out.println("Read output: " + value);
	}
}
