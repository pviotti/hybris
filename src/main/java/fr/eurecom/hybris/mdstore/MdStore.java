package fr.eurecom.hybris.mdstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.Config;

/**
 * Class that wraps the Zookeeper client.
 * Provides r&w access to the metadata storage.
 * @author p.viotti
 */
public class MdStore extends SyncPrimitive {
    
    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);
    
    private String storageRoot;
    
    /**
     * Constructs a new MdStore.
     * @param address the address of ZK server
     * @param storageRoot the hybris metadata root folder
     * @throws IOException thrown in case of error while initializing the ZK client
     */
    public MdStore(String address, String storageRoot) throws IOException {
        
        super(address);
        this.storageRoot = "/" + storageRoot;
        
        try {
            Stat stat = zk.exists(this.storageRoot, false);
            if (stat == null) {
                logger.debug("Creating root dir...");
                zk.create(this.storageRoot, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            logger.error("KeeperException, could not initialize the Zookeeper client. " + e.getMessage(), e);
            throw new IOException(e);
        } catch (InterruptedException e) {
            logger.error("InterruptedException, could not initialize the Zookeeper client.", e);
            throw new IOException(e);
        }
    }
    
    // =======================================================================================
    //                                      PUBLIC APIs
    // ---------------------------------------------------------------------------------------
    
    // TODO should we add a flag for explicitly overwriting existing nodes 
    // (or, viceversa, for explicitly creating a new node)? 
    // TODO should we care about ZK node version upon writing to existing node?    
    public void tsWrite(String key, TsDir tsdir) throws IOException {
        
        String path = this.storageRoot + "/" + key;
        try {
            
            Stat stat = zk.exists(path, false);
            if (stat == null) {
                zk.create(path, tsdir.serialize(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.debug("ZNode {} created.", key);
            } else {
                zk.setData(path, tsdir.serialize(), -1);    // overwrite no matter which version
                logger.debug("ZNode {} overwritten.", key);
            }
                    
        } catch (KeeperException e) {
            logger.error("InterruptedException, could not read the key.", e);
            throw new IOException(e);
        } catch (InterruptedException e) {
            logger.error("InterruptedException, could not read the key.", e);
            throw new IOException(e);
        }
    }
    
    
    public byte[] tsRead(String key) {
        
        String path = this.storageRoot + "/" + key;
        try {
            zk.sync(path, null, null);      // NOTE: There is no synchronous version of this ZK API 
                                            // (https://issues.apache.org/jira/browse/ZOOKEEPER-1167ordering) 
                                            // however, order guarantees among operations allow not to wait for asynchronous callback to be called
            return zk.getData(path, false, new Stat());
        } catch (KeeperException e) {    // XXX check how often / when it happens
            
            if (e.code() == KeeperException.Code.NONODE) {    // XXX implement a retry mechanisms if KeeperException.Code.NONODE ?
                logger.warn("KeeperException, could not find the specified znode. " + e.getMessage(), e);
                return null;
            } else {
                logger.error("KeeperException, could not read the key. " + e.getMessage(), e);
                return null;
            }
            
        } catch (InterruptedException e) {    // XXX check how often / when it happens
            logger.error("InterruptedException, could not read the key.", e);
            return null;
        }
    }
    
    
    public List<String> list(String key) throws IOException {
        try {
            return zk.getChildren(key, false);
        } catch (KeeperException e) {         // XXX check how often / when it happens
            logger.error("KeeperException, could not list the children of key " + key, e);
            throw new IOException(e);
        } catch (InterruptedException e) {    // XXX check how often / when it happens
            logger.error("InterruptedException, could not list the children of key " + key, e);
            throw new IOException(e);
        }
    }
    
    
    public void delete(String key) throws IOException {
        try {
            zk.delete(key, -1);        // delete no matter which version
        } catch (KeeperException e) {        // XXX check how often / when it happens
            logger.error("KeeperException, could not delete the key " + key, e);
            throw new IOException(e);
        } catch (InterruptedException e) {    // XXX check how often / when it happens
            logger.error("InterruptedException, could not delete the key " + key, e);
            throw new IOException(e);
        }
    }
    
    
    /**
     * TODO TEMP for dev purposes
     */
//    public static void main(String[] args) throws IOException {
//        MdStore mds = new MdStore(    Config.getInstance().getProperty(Config.ZK_ADDR),
//                                    Config.getInstance().getProperty(Config.ZK_ROOT)    );
//        ArrayList<String> replicas = new ArrayList<String>(Arrays.asList("Amazon", "Azure"));
//        mds.tsWrite("test_001", new TsDir(System.currentTimeMillis(), "hashvalue".getBytes(), replicas));
//        TsDir output = new TsDir(mds.tsRead("test_001"));
//        System.out.println("OUTPUT: " + output);
//    }
}