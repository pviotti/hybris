package fr.eurecom.hybris.mds;

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
import fr.eurecom.hybris.mds.Metadata.Timestamp;

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
    
    /* ---------------------------------------------------------------------------------------
                                            Public APIs
       --------------------------------------------------------------------------------------- */

    public void tsWrite(String key, Metadata md) throws HybrisException {
        
        String path = this.storageRoot + "/" + key;
        try {
            
            zk.setData(path, md.serialize(), md.getTs().getNum() - 1);
            return;
            
        } catch (KeeperException e) {
            
            if (e.code() == KeeperException.Code.NONODE) {              // the znode does not exist: let's create it
                
                try {
                    zk.create(path, md.serialize(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    logger.debug("ZNode {} created.", path);
                    return;
                } catch (KeeperException e1) {
                    throw new HybrisException(e1);                      // the node already exist, concurrently created by someone else
                } catch (InterruptedException e1) {
                    throw new HybrisException(e1);
                }
                
            } else if (e.code() == KeeperException.Code.BADVERSION) {   // the znode version does not match 
                
                Stat stat = new Stat();
                byte[] newValue = null;
                try {
                    newValue = zk.getData(path, false, stat);           // XXX does it need to sync before
                } catch (KeeperException e1) {
                    
                    if (e.code() != KeeperException.Code.NONODE) {  // FIXME should not happen: 
                                                                    // tomb stone value instead of deleting the znode when gc
                        logger.debug("Version mismatch on writing {}, but value deleted by concurrent gc: retrying.", key);
                        tsWrite(key, md);       
                        return;
                    } else 
                        throw new HybrisException(e1);
                    
                } catch (InterruptedException e1) {
                    throw new HybrisException(e1);
                }
                
                Metadata newmd = new Metadata(newValue);
                if (md.getTs().isGreater(newmd.getTs())) {      // Concurrent clients trying to overwrite the same version
                    logger.debug("Version mismatch on writing {}, found previous version ({}): retrying.", key, newmd.getTs());
                    md.setTs(new Timestamp(stat.getVersion() + 1, md.getTs().getCid()));
                    tsWrite(key, md);
                    return;
                } else {                                        // The version which I'm trying to overwrite is obsolete: fail
                    logger.debug("Version mismatch on writing {}, found more recent version ({}): failing.", key, newmd.getTs());
                    throw new HybrisException("KeeperException, could not write the key.", e);
                }
                
            } else {
                logger.error("KeeperException, could not write the key.", e);
                throw new HybrisException("KeeperException, could not write the key.", e);
            }
        } catch (InterruptedException e) {
            logger.error("InterruptedException, could not write the key.", e);
            throw new HybrisException("InterruptedException, could not write the key. " + e.getMessage(), e);
        }
    }
    
    
    public byte[] tsRead(String key) throws HybrisException {
        
        String path = this.storageRoot + "/" + key;
        try {
            zk.sync(path, null, null);      // NOTE: There is no synchronous version of this ZK API (https://issues.apache.org/jira/browse/ZOOKEEPER-1167ordering) 
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
            zk.delete(path, -1);             // Notice: delete no matter which version
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