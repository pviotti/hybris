package fr.eurecom.hybris.mdstore;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.HybrisException;

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
    public void tsWrite(String key, TsDir tsdir) throws HybrisException {
        
        String path = this.storageRoot + "/" + key;
        try {
            
            Stat stat = zk.exists(path, false);
            if (stat == null) {
                zk.create(path, tsdir.serialize(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.debug("ZNode {} created.", path);
            } else {
                zk.setData(path, tsdir.serialize(), tsdir.getTs().getNum());
                logger.debug("ZNode {} overwritten.", path);
            }
                    
        } catch (KeeperException e) {
            
            if (e.code() == KeeperException.Code.BADVERSION) {
                logger.warn("Write failed due to the wrong version ({}) of key {}", tsdir.getTs().getNum(), key);
                
                try {
                    byte[] newValue = tsRead(key);
                    if (newValue != null) {
                        TsDir newtsdir = new TsDir(newValue);
                        if (tsdir.getTs().isGreater(newtsdir.getTs())) {
                            tsWrite(key, tsdir);
                        } // XXX Wrong version but no need to retry
                    }
                } catch (HybrisException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                } 
            }
            // TODO Auto-generated catch block
            else e.printStackTrace();
            
            logger.error("KeeperException, could not write the key.", e);
            throw new HybrisException("KeeperException, could not write the key.", e);
        } catch (InterruptedException e) {
            logger.error("InterruptedException, could not write the key.", e);
            throw new HybrisException("InterruptedException, could not write the key. " + e.getMessage(), e);
        }
    }
    
    
    public byte[] tsRead(String key) throws HybrisException {
        
        String path = this.storageRoot + "/" + key;
        try {
            zk.sync(path, null, null);      // NOTE: There is no synchronous version of this ZK API 
                                            // (https://issues.apache.org/jira/browse/ZOOKEEPER-1167ordering) 
                                            // however, order guarantees among operations allow not to wait for asynchronous callback to be called
            return zk.getData(path, false, null);
        } catch (KeeperException e) {
            
            if (e.code() == KeeperException.Code.NONODE)
                return null;
            else {
                logger.error("KeeperException, could not read the ZNode " + path, e);
                throw new HybrisException("KeeperException, could not read the ZNode " + path, e);
            }
            
        } catch (InterruptedException e) {
            logger.error("InterruptedException, could not read the ZNode " + path, e);
            throw new HybrisException("InterruptedException, could not read the key. " + e.getMessage(), e);
        }
    }
    
    
    public List<String> list(String key) throws HybrisException {
        
        String path = this.storageRoot + "/" + key;
        try {
            return zk.getChildren(path, false);
        } catch (KeeperException e) {
            
            if (e.code() == KeeperException.Code.NONODE)
                return null;
            else {
                logger.error("KeeperException, could not list the children of ZNode " + path, e);
                throw new HybrisException("KeeperException, could not list the children of ZNode " + path, e);
            }
            
        } catch (InterruptedException e) {
            logger.error("InterruptedException, could not list the children of ZNode " + path, e);
            throw new HybrisException("InterruptedException, could not list the children of ZNode " + path, e);
        }
    }
    
    
    public void delete(String key) throws HybrisException {
        
        String path = this.storageRoot + "/" + key;
        try {
            zk.delete(path, -1);             // XXX delete no matter which version
        } catch (KeeperException e) {
            
            if (e.code() == KeeperException.Code.NONODE)
                return;
            else {
                logger.error("KeeperException, could not delete the ZNode " + path, e);
                throw new HybrisException("KeeperException, could not list the children of ZNode " + path, e);
            }
            
        } catch (InterruptedException e) {
            logger.error("InterruptedException, could not delete the ZNode " + path, e);
            throw new HybrisException("InterruptedException, could not delete the ZNode " + path, e);
        }
    }
}