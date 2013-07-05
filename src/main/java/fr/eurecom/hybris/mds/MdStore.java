package fr.eurecom.hybris.mds;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
    public static int NONODE = -1;      // integer marker to tell whether a znode has to be created 
    
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
                zk.create(this.storageRoot, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.debug("Created root metadata container \"{}\".", this.storageRoot);
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

    /**
     * Timestamped write on metadata storage.
     * @param key - the key
     * @param md - the metadata to be written
     * @param zkVersion - the znode version expected to be overwritten; -1 when the znode does not exist
     * @throws HybrisException
     */
    public void tsWrite(String key, Metadata md, int zkVersion) throws HybrisException {
        
        String path = this.storageRoot + "/" + key;
        try {
            if (zkVersion == NONODE){
                zk.create(path, md.serialize(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.debug("ZNode {} created.", path);
                return;
            } else {
                zk.setData(path, md.serialize(), zkVersion);
                logger.debug("ZNode {} modified.", path);
                return;
            }
        } catch (KeeperException e) {       // NONODE exception should not happen since we set a tombstone value upon deletion
            
            if ((e.code() == KeeperException.Code.NODEEXISTS) ||            // multiple clients tried to create
                    (e.code() == KeeperException.Code.BADVERSION)) {        // or modify the same znode concurrently 
          
                Stat stat = new Stat();
                byte[] newValue = null;
                try {
                    newValue = zk.getData(path, false, stat);
                } catch (KeeperException e1) {
                    throw new HybrisException(e1);
                } catch (InterruptedException e1) {
                    throw new HybrisException(e1);
                }
      
                Metadata newmd = new Metadata(newValue);
                if (md.getTs().isGreater(newmd.getTs())) {
                    logger.debug("Found smaller version ({}) writing {}: retrying.", newmd.getTs(), key);
                    tsWrite(key, md, stat.getVersion());
                    return;
                } else {
                    logger.debug("Found greater version ({}) writing {}: failing.", newmd.getTs(), key);
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
    
    
    /**
     * Timestamped read from metadata storage.
     * @param key the key to read
     * @param stat the Stat Zookeeper object to be written with znode details
     * @return Metadata object 
     *              or null in case the znode does not exist or there is a tombstone Metadata object
     *              (to distinguish these two cases one must use the Stat object)
     * @throws HybrisException
     */
    public Metadata tsRead(String key, Stat stat) throws HybrisException {
        
        String path = this.storageRoot + "/" + key;
        try {
            zk.sync(path, null, null);      // NOTE: There is no synchronous version of this ZK API (https://issues.apache.org/jira/browse/ZOOKEEPER-1167ordering) 
                                            // however, order guarantees among operations allow not to wait for asynchronous callback to be called
            byte[] rawMd = zk.getData(path, false, stat);
            Metadata md = new Metadata(rawMd);
            if (!md.isTombstone())
                return md;
            return null;
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
    
    
    /**
     * Get all the stored metadata (filtering out tombstone values).
     * @return a map of keys (String) and Metadata objects
     * @throws HybrisException
     */
    public Map<String, Metadata> getAll() throws HybrisException {
        
        List<String> znodes;
        try {
            znodes = zk.getChildren(this.storageRoot, false);
        } catch (KeeperException | InterruptedException e1) {
            logger.error("Could not list the children of znode " + this.storageRoot, e1);
            throw new HybrisException(e1);
        }
        
        HashMap<String, Metadata> retMap = new HashMap<String, Metadata>();
        for (String znode : znodes) {
            String znodePath = this.storageRoot + "/" + znode;
            byte[] rawMd = null;
            try {
                rawMd = zk.getData(znodePath, false, null);
            } catch (KeeperException | InterruptedException e) {
                logger.warn("Could not read metadata for key " + znodePath, e);
            }
            Metadata md = new Metadata(rawMd);
            if (!md.isTombstone())
                retMap.put(znode, md);
        }
        return retMap;
    }
    
    
    /**
     * Get the list of metadata keys stored (filtering out tombstone values).
     * @return the list of metadata keys stored in the 
     * @throws HybrisException
     */
    public List<String> list() throws HybrisException {
        
        List<String> znodes;
        try {
            znodes = zk.getChildren(this.storageRoot, false);
        } catch (KeeperException | InterruptedException e1) {
            logger.error("Could not list the children of znode " + this.storageRoot, e1);
            throw new HybrisException(e1);
        }
        
        for (Iterator<String> it = znodes.iterator(); it.hasNext(); ) {
            String znode = it.next();
            String znodePath = this.storageRoot + "/" + znode;
            byte[] rawMd = null;
            try {
                rawMd = zk.getData(znodePath, false, null);
            } catch (KeeperException | InterruptedException e) {
                logger.warn("Could not read metadata for key " + znodePath, e);
            }
            Metadata md = new Metadata(rawMd);
            if (md.isTombstone())
                it.remove();
        }
        return znodes;
    }
    

    /**
     * Mark a key as deleted (write a tombstone value).
     * @param key
     * @throws HybrisException
     */
    public void delete(String key) throws HybrisException {
        
        String path = this.storageRoot + "/" + key;
        try {
            Metadata tombstoneMd = new Metadata(null, null, null);
            zk.setData(path, tombstoneMd.serialize(), -1);
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
    
    
    /**
     * Empty the metadata storage root container.
     * ATTENTION: it erases all metadata stored in the root container!
     * @throws HybrisException
     */
    public void emptyStorageContainer() throws HybrisException {
        
        String path = this.storageRoot;
        try {
            Stat s = zk.exists(path, false);
            if (s != null) {
                List<String> children = zk.getChildren(path, false);
                for (String child : children) {
                    String node = path + "/" + child;
                    recursiveDelete(node);
                }
            }
        } catch (KeeperException e) {
            logger.warn("KeeperException, could not delete the ZNode " + path, e);
            throw new HybrisException("KeeperException, could not list the children of ZNode " + path, e);
        } catch (InterruptedException e) {
            logger.warn("InterruptedException, could not delete the ZNode " + path, e);
            throw new HybrisException("InterruptedException, could not delete the ZNode " + path, e);
        }
    }
    
    
    /* ---------------------------------------------------------------------------------------
                                        Private methods
       --------------------------------------------------------------------------------------- */
    
    
    private void recursiveDelete(String key) throws KeeperException, InterruptedException {
        Stat s = zk.exists(key, false);
        if (s != null) {
            List<String> children = zk.getChildren(key, false);
            for (String child : children) {
                String node = key + "/" + child;
                recursiveDelete(node);
            }
            zk.delete(key, -1);         // delete no matter which version
        }
    }
}