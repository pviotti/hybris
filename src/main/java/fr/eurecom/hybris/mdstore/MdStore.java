package fr.eurecom.hybris.mdstore;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;


public class MdStore extends SyncPrimitive {
	
	int size;
	String clientName;
	Stat stat = null;
	
	String storageRoot;
	int tsVersion = 0;

	public MdStore(String address, String storageRoot) {
		super(address);
		this.storageRoot = storageRoot;

		// Create MDS service nodes
		if (zk != null) {
			try {
				stat = zk.exists(storageRoot, false);
				if (stat == null) {
					System.out.println("creating MDSdir...");
					zk.create(storageRoot, new byte[0], Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
					stat = zk.exists(storageRoot, false);
				}
				tsVersion = stat.getVersion();
			} catch (KeeperException e) {
				System.out
				.println("Keeper exception when instantiating MDS: "
						+ e.toString());
			} catch (InterruptedException e) {
				System.out.println("Interrupted exception");
			}
		}

		// My node name
		try {
			clientName = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
			System.out.println("Client name: " + clientName);
		} catch (UnknownHostException e) {
			System.out.println(e.toString());
		}
	}

	public void tsWrite(TsDir tsdir) {
		try {
			stat = zk.setData(storageRoot, tsdir.serialize(), tsVersion);
			System.out.println("Good version " + tsVersion);
		} catch (KeeperException e) {
			if (e.code() == KeeperException.Code.BADVERSION) {
				System.out.println("Wrong version " + tsVersion);
				try {
					TsDir latest = new TsDir(zk.getData(storageRoot, false, stat));
					tsVersion = stat.getVersion();
					if (tsdir.getTs().isGreater(latest.getTs())) {
						this.tsWrite(tsdir);
					} else {
						System.out.println("Wrong version but no need to retry");
					}
				} catch (KeeperException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			// TODO Auto-generated catch block
			else e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public byte[] tsRead(String key) {
		
		String path = storageRoot + "/" + key;
		
		try {
			zk.sync(path, null, null);	// There is no synchronous version of this ZK API 
										// (https://issues.apache.org/jira/browse/ZOOKEEPER-1167ordering) 
										// however, order guarantees among operations allow not to wait for asynchronous callback to be called
			
			Stat stat = new Stat();
			byte[] out = zk.getData(path, false, stat);
			//System.out.println(stat);
			return out;
			//return new TsDir( zk.getData(path, false, stat) );
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void write(TsDir tsdir, byte[] hash) {
		String znode = storageRoot + "/" + tsdir.getTs().getCid() + "/" + tsdir.getTs().getNum();
		try {
			zk.create(znode, hash, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public byte[] read(TsDir tsdir) {
		String znode = storageRoot + "/" + tsdir.getTs().getCid() + "/" + tsdir.getTs().getNum();
		byte[] ret = null;
		try {
			ret = zk.getData(znode, false, stat);
		} catch (KeeperException e) { 
			if (e.code() == KeeperException.Code.NONODE) {
				zk.sync(znode, null, null);
				try {
					ret = zk.getData(znode, false, stat);
				} catch (KeeperException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
	}
	
	
	public static void main(String[] args) {
		MdStore mds = new MdStore("127.0.0.1:2181","/MdStore");
		String output = new String(mds.tsRead("prova"));
		System.out.println("OUTPUT: " + output);
	}
}