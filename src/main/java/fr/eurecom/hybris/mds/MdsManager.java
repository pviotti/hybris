package fr.eurecom.hybris.mds;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.mds.Metadata.Timestamp;

/**
 * Wraps the Zookeeper client and
 * Provides read&write access to the metadata storage.
 * @author P. Viotti
 */
public class MdsManager implements ConnectionStateListener {

    private static final Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private final CuratorFramework zkCli;
    private final String storageRoot;

    /* Integer marker to tell whether a znode has to be created.
     * As Zookeeper setData API parameter it implies overwriting no matter which znode version.
     */
    public static int NONODE = -1;

    private final String gcRoot;
    private final String gcStaleDir;
    private final String gcOrphansDir;

    enum GcType { STALE, ORPHAN };

    /**
     * Constructs a new MdsManager.
     * @param zkConnectionStr Zookeeper cluster connection string (e.g. "zksrv1.net:2181,zksrv2.net:2181")
     * @param zkRoot the Hybris metadata root folder
     * @throws IOException thrown in case of error while initializing the Zookeeper client
     */
    public MdsManager(String zkConnectionStr, String zkRoot) throws IOException {

        this.storageRoot = "/" + zkRoot;

        this.gcRoot = this.storageRoot + "-gc";
        this.gcStaleDir = this.gcRoot + "/stale";
        this.gcOrphansDir = this.gcRoot + "/orphans";

        try {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            this.zkCli = CuratorFrameworkFactory.newClient(zkConnectionStr, retryPolicy);
            this.zkCli.getConnectionStateListenable().addListener(this);
            this.zkCli.start();

            for (String dir : new String[]{ this.storageRoot, this.gcRoot,
                    this.gcStaleDir, this.gcOrphansDir })
                try {
                    this.zkCli.create().forPath(dir);
                    logger.debug("Created {}.", dir);
                } catch (KeeperException e) {
                    if (e.code() != KeeperException.Code.NODEEXISTS)
                        throw e;
                }

        } catch (Exception e) {
            logger.error("Could not initialize the Zookeeper client. " + e.getMessage(), e);
            throw new IOException(e);
        }
    }


    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (!ConnectionState.CONNECTED.equals(newState))
            logger.warn("Zookeeper connection state changed to: " + newState);
    }


    /**
     * Worker thread class in charge of marking stale and orphan keys.
     */
    public class GcMarker extends Thread {

        private final String key;
        private Timestamp ts;
        private List<Kvs> replicas;
        private final GcType type;

        public GcMarker(String key, Timestamp ts,
                List<Kvs> savedReplicas) {
            this.key = key;
            this.ts = ts;
            this.replicas = savedReplicas;
            this.type = GcType.ORPHAN;
        }

        public GcMarker(String key) {
            this.key = key;
            this.type = GcType.STALE;
        }

        public void run() {

            String path;
            switch(this.type) {

                case STALE:
                    // create ZNode  <root>-gc/stale/<key>
                    path = MdsManager.this.gcStaleDir + "/" + this.key;
                    try {
                        MdsManager.this.zkCli.create().forPath(path);
                        logger.debug("GcMarker: marked {} as stale", path);
                    } catch (KeeperException e){
                        if (e.code() != KeeperException.Code.NODEEXISTS)
                            logger.warn("GcMarker: could not create stale node " + path, e);
                    } catch (Exception e) {
                        logger.warn("GcMarker: could not create stale node " + path, e);
                    }
                    break;
                case ORPHAN:
                    // create ZNode <root>-gc/orphans/<KvsKey>
                    path = MdsManager.this.gcOrphansDir + "/" + Utils.getKvsKey(this.key, this.ts);
                    byte[] value = new Metadata(this.ts, null, 0, this.replicas).serialize();
                    try {
                        MdsManager.this.zkCli.create().forPath(path, value);
                        logger.debug("GcMarker: marked {} as orphan", path);
                    } catch (KeeperException e){
                        if (e.code() != KeeperException.Code.NODEEXISTS)
                            logger.warn("GcMarker: could not create orphan node " + path, e);
                    } catch (Exception e) {
                        logger.warn("GcMarker: could not create orphan node " + path, e);
                    }
                    break;
            }
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
     * @return boolean: true if a znode has been modified and stale old values need to be garbage-collected
     *                  false otherwise: a new znode has been created
     * @throws HybrisException
     */
    public boolean tsWrite(String key, Metadata md, int zkVersion) throws HybrisException {

        String path = this.storageRoot + "/" + key;
        try {
            if (zkVersion == NONODE){
                this.zkCli.create().forPath(path, md.serialize());
                logger.debug("ZNode {} created.", path);
                return false;
            } else {
                this.zkCli.setData().withVersion(zkVersion).forPath(path, md.serialize());
                logger.debug("ZNode {} modified.", path);
                return true;
            }
        } catch (KeeperException e) {       // NONODE exception should not happen since we set a tombstone value upon deletion

            if (e.code() == KeeperException.Code.NODEEXISTS ||            // multiple clients tried to create
                    e.code() == KeeperException.Code.BADVERSION) {        // or modify the same znode concurrently

                Stat stat = new Stat();
                byte[] newValue = null;
                try {
                    newValue = this.zkCli.getData().storingStatIn(stat).forPath(path);
                } catch (Exception e1) {
                    throw new HybrisException(e1);
                }

                Metadata newmd = new Metadata(newValue);
                if (md.getTs().isGreater(newmd.getTs())) {
                    logger.debug("Found smaller version ({}) writing {}: retrying.", newmd.getTs(), key);
                    return this.tsWrite(key, md, stat.getVersion());
                } else {
                    logger.warn("Found greater version ({}) writing {}: failing.", newmd.getTs(), key);
                    return false;   // XXX
                    // throw new HybrisException("KeeperException, could not write the key.", e);
                }

            } else {
                logger.error("Could not write ZNode " + key);
                throw new HybrisException("Could not write the ZNode " + key, e);
            }

        } catch (Exception e) {
            logger.error("Could not write ZNode " + key, e);
            throw new HybrisException("Could not write ZNode " + key + ": " + e.getMessage(), e);
        }
    }


    /**
     * Timestamped read ("slow read" in ZooKeeper parlance) from metadata storage.
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
            this.zkCli.sync().forPath(path);
            byte[] rawMd = this.zkCli.getData().storingStatIn(stat).forPath(path);
            return new Metadata(rawMd);
        } catch (KeeperException e) {

            if (e.code() == KeeperException.Code.NONODE)
                return null;
            else {
                logger.error("Could not read ZNode " + path, e);
                throw new HybrisException("Could not read the ZNode " + path, e);
            }

        } catch (Exception e) {
            logger.error("Could not read ZNode " + path, e);
            throw new HybrisException("Could not read the ZNode " + path + e.getMessage(), e);
        }
    }


    /**
     * Get the list of metadata keys stored (filtering out tombstone values).
     * @return the list of metadata keys stored in the
     * @throws HybrisException
     */
    public List<String> list() throws HybrisException {

        List<String> znodes;
        try {
            znodes = this.zkCli.getChildren().forPath(this.storageRoot);
        } catch (Exception e) {
            logger.error("Could not list the children of ZNode " + this.storageRoot, e);
            throw new HybrisException(e);
        }

        for (Iterator<String> it = znodes.iterator(); it.hasNext(); ) {
            String znode = it.next();
            String znodePath = this.storageRoot + "/" + znode;
            byte[] rawMd = null;
            try {
                rawMd = this.zkCli.getData().forPath(znodePath);
            } catch (Exception e) {
                logger.warn("Could not read metadata for ZNode " + znodePath, e);
            }
            Metadata md = new Metadata(rawMd);
            if (md.isTombstone())
                it.remove();
        }
        return znodes;
    }


    /**
     * Mark a key as deleted writing a tombstone value.
     * @param key
     * @param tombstone the tombstone metadata to be written
     * @param zkVersion
     * @throws HybrisException
     */
    public void delete(String key, Metadata tombstone, int zkVersion) throws HybrisException {
        this.tsWrite(key, tombstone, zkVersion);
    }


    /**
     * Get all the stored metadata (filtering out tombstone values).
     * @return a map of keys (String) and Metadata objects
     * @throws HybrisException
     */
    public Map<String, Metadata> getAll() throws HybrisException {
        try {
            return this.getAllChildrenMetadata(this.storageRoot);
        } catch (Exception e) {
            logger.error("Could not get all the metadata from children ZNodes of " + this.gcRoot, e);
            throw new HybrisException(e);
        }
    }


    /**
     * Empty the metadata storage root container.
     * ATTENTION: it erases all metadata stored in the root container!
     * @throws HybrisException
     */
    public void emptyMetadataContainer() throws HybrisException {

        try {
            List<String> znodes = this.zkCli.getChildren().forPath(this.storageRoot);
            for (String key : znodes) {
                String path = this.storageRoot + "/" + key;
                this.recursiveDelete(path);
            }
        } catch (Exception e) {
            logger.warn("Could not empty the root container", e);
            throw new HybrisException("Could not empty the root container", e);
        }
    }


    public void shutdown() {
        this.zkCli.close();
    }


    /* -------------------------------------- GC functions -------------------------------------- */

    /**
     * Get orphan keys and their metadata.
     * @return
     * @throws HybrisException
     */
    public Map<String, Metadata> getOrphans() throws HybrisException {
        try {
            return this.getAllChildrenMetadata(this.gcOrphansDir);
        } catch (Exception e) {
            logger.error("Could not get all the metadata from children ZNodes of " + this.gcOrphansDir, e);
            throw new HybrisException(e);
        }
    }


    /**
     * Get stale keys.
     * @return
     * @throws HybrisException
     */
    public List<String> getStaleKeys() throws HybrisException {

        try {
            return this.zkCli.getChildren().forPath(this.gcStaleDir);
        } catch (Exception e) {
            logger.error("Could not list the children of ZNode " + this.storageRoot, e);
            throw new HybrisException(e);
        }
    }


    /**
     * Delete the set of orphan keys passed as argument.
     * @param orphanKeys
     */
    public void removeOrphanKeys(Set<String> orphanKeys) {

        for (String key : orphanKeys) {
            String znodePath = this.gcOrphansDir + "/" + key;
            try {
                this.zkCli.delete().forPath(znodePath);
            } catch (KeeperException e) {
                if (e.code() != KeeperException.Code.NONODE)
                    logger.warn("Could not delete orphan ZNode " + znodePath, e);
            } catch (Exception e) {
                logger.warn("Could not delete orphan ZNode " + znodePath, e);
            }
        }
    }


    /**
     * Delete the key passed as argument from the list of keys
     * to be checked as outdated for gc.
     * @param staleKeys
     */
    public void removeStaleKey(String staleKey) {

        String znodePath = this.gcStaleDir + "/" + staleKey;
        try {
            this.zkCli.delete().forPath(znodePath);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NONODE)
                logger.warn("Could not delete orphan ZNode " + znodePath, e);
        } catch (Exception e) {
            logger.warn("Could not delete orphan ZNode " + znodePath, e);
        }
    }


    /**
     * Empty stale and orphan keys containers.
     * @throws HybrisException
     */
    public void emptyStaleAndOrphansContainers() throws HybrisException {

        try {
            for (String path : new String[]{this.gcOrphansDir, this.gcStaleDir}) {
                List<String> znodes = this.zkCli.getChildren().forPath(path);
                for (String key : znodes) {
                    String znode = path + "/" + key;
                    this.recursiveDelete(znode);
                }
            }
        } catch (Exception e) {
            logger.warn("Could not empty the GC containers", e);
            throw new HybrisException("Could not empty the GC containers", e);
        }
    }


    /* ---------------------------------------------------------------------------------------
                                        Private methods
       --------------------------------------------------------------------------------------- */

    /**
     * Retrieve all Metadata objects associated to the children of a certain key.
     * @param key
     * @return a Map<String, Metadata> of all metadata associated with the ZNode children of key
     * @throws Exception
     */
    public Map<String, Metadata> getAllChildrenMetadata(String key) throws Exception {

        List<String> znodes;
        try {
            znodes = this.zkCli.getChildren().forPath(key);
        } catch (Exception e) {
            throw e;
        }

        HashMap<String, Metadata> retMap = new HashMap<String, Metadata>();
        for (String znode : znodes) {
            String znodePath = key + "/" + znode;
            byte[] rawMd = null;
            try {
                rawMd = this.zkCli.getData().forPath(znodePath);
            } catch (Exception e) {
                logger.warn("Could not read metadata for ZNode " + znodePath, e);
            }
            Metadata md = new Metadata(rawMd);
            if (!md.isTombstone())
                retMap.put(znode, md);
        }
        return retMap;
    }


    /**
     * Recursively delete a key and all its children.
     * @param key
     * @throws Exception
     */
    private void recursiveDelete(String key) throws Exception {
        Stat s = this.zkCli.checkExists().forPath(key);
        if (s != null) {
            List<String> children = this.zkCli.getChildren().forPath(key);
            for (String child : children) {
                String node = key + "/" + child;
                this.recursiveDelete(node);
            }
            this.zkCli.delete().forPath(key);
        }
    }
}