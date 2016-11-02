/**
 * Copyright (C) 2013 EURECOM (www.eurecom.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.eurecom.hybris.mds;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.Hybris.HybrisWatcher;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.mds.Metadata.Timestamp;

/**
 * Reliable Metadata Store based on ZooKeeper.
 * 
 * @author P. Viotti
 */
public class ZkRmds implements Rmds, ConnectionStateListener {

	private static final Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

	private final CuratorFramework zkCli;
	private final String storageRoot;

	/*
	 * Integer marker to tell whether a metadata node has to be created. Used as
	 * Zookeeper setData API parameter, it implies overwriting no matter which
	 * znode version.
	 */
	public static int NONODE = -1;

	private final String gcRoot;
	private final String gcStaleDir;
	private final String gcOrphansDir;
	
	private final boolean quorumRead;

	/**
	 * Constructs a new MdsManager.
	 * 
	 * @param zkConnectionStr
	 *            Zookeeper cluster connection string (e.g.
	 *            "zksrv1.net:2181,zksrv2.net:2181")
	 * @param zkRoot
	 *            the Hybris metadata root folder
	 * @throws IOException
	 *             thrown in case of error while initializing the Zookeeper
	 *             client
	 */
	public ZkRmds(String zkConnectionStr, String zkRoot, boolean qRead) throws IOException {

		this.storageRoot = "/" + zkRoot;

		this.gcRoot = this.storageRoot + "-gc";
		this.gcStaleDir = this.gcRoot + "/stale";
		this.gcOrphansDir = this.gcRoot + "/orphans";
		this.quorumRead = qRead;

		try {
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
			this.zkCli = CuratorFrameworkFactory.newClient(zkConnectionStr, retryPolicy);
			this.zkCli.getConnectionStateListenable().addListener(this);
			this.zkCli.start();

			for (String dir : new String[] { this.storageRoot, this.gcRoot, this.gcStaleDir, this.gcOrphansDir })
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * fr.eurecom.hybris.mds.Mds1#stateChanged(org.apache.curator.framework.
	 * CuratorFramework, org.apache.curator.framework.state.ConnectionState)
	 */
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		if (!ConnectionState.CONNECTED.equals(newState))
			logger.warn("Zookeeper connection state changed to: " + newState);
	}

	/*
	 * -------------------------------------------------------------------------
	 * Public APIs
	 * -------------------------------------------------------------------------
	 */

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#tsWrite(java.lang.String,
	 * fr.eurecom.hybris.mds.Metadata, int)
	 */
	@Override
	public boolean tsWrite(String key, Metadata md, long zkVersion) throws HybrisException {

		String path = this.storageRoot + "/" + key;
		try {
			if (zkVersion == NONODE) {
				this.zkCli.create().forPath(path, md.serialize());
				logger.debug("ZNode {} created.", path);
				return false;
			} else {
				this.zkCli.setData().withVersion((int) zkVersion).forPath(path, md.serialize());
				logger.debug("ZNode {} modified.", path);
				return true;
			}
		} catch (KeeperException e) { // NONODE exception should not happen
										// since we set a tombstone value upon
										// deletion

			if (e.code() == KeeperException.Code.NODEEXISTS || // multiple
																// clients tried
																// to create
					e.code() == KeeperException.Code.BADVERSION) { // or modify
																	// the same
																	// znode
																	// concurrently

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
					logger.debug("Found greater version ({}) writing {}: overwritten.", newmd.getTs(), key);
					return false;
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#tsMultiWrite(java.util.LinkedHashMap,
	 * java.util.LinkedHashMap)
	 */
	@Override
	public boolean tsMultiWrite(LinkedHashMap<String, Metadata> mdMap, LinkedHashMap<String, Stat> statMap)
			throws HybrisException {

		try {
			CuratorTransaction curatorTransaction = this.zkCli.inTransaction();

			for (Entry<String, Metadata> entry : mdMap.entrySet()) {
				int ver = statMap.get(entry.getKey()).getVersion();
				if (ver == NONODE) {
					curatorTransaction = curatorTransaction.create()
							.forPath(this.storageRoot + "/" + entry.getKey(), entry.getValue().serialize()).and();
				} else {
					curatorTransaction = curatorTransaction.setData().withVersion(ver)
							.forPath(this.storageRoot + "/" + entry.getKey(), entry.getValue().serialize()).and();
				}
			}

			if (curatorTransaction instanceof CuratorTransactionFinal)
				((CuratorTransactionFinal) curatorTransaction).commit();

			return true; // XXX

		} catch (KeeperException e) { // NONODE exception should not happen
										// since we set a tombstone value upon
										// deletion

			// XXX see if it is worth retrying

			logger.error("Could not perform transactional timestamped write.", e);
			throw new HybrisException("Could not perform transactional timestamped write: " + e.getMessage(), e);

		} catch (Exception e) {
			logger.error("Could not perform transactional timestamped write.", e);
			throw new HybrisException("Could not perform transactional timestamped write: " + e.getMessage(), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#tsRead(java.lang.String,
	 * org.apache.zookeeper.data.Stat)
	 */
	public Metadata tsRead(String key, Stat stat) throws HybrisException {

		String path = this.storageRoot + "/" + key;
		try {
			if (quorumRead)
				this.zkCli.setData().forPath(this.storageRoot, new byte[]{(byte) 0x00});
			else
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
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#tsRead(java.lang.String,
	 * org.apache.zookeeper.data.Stat,
	 * org.apache.curator.framework.api.CuratorWatcher)
	 */
	public Metadata tsRead(String key, Stat stat, HybrisWatcher watcher) throws HybrisException {

		String path = this.storageRoot + "/" + key;
		try {
			if (quorumRead)
				this.zkCli.setData().forPath(this.storageRoot, new byte[]{(byte) 0x00});
			else
				this.zkCli.sync().forPath(path);
			byte[] rawMd = this.zkCli.getData().storingStatIn(stat).usingWatcher(watcher).forPath(path);
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#tsMultiRead(java.util.LinkedHashMap)
	 */
	public LinkedHashMap<String, Metadata> tsMultiRead(LinkedHashMap<String, Stat> lst) throws HybrisException {
		try {
			LinkedHashMap<String, Metadata> mdLst = new LinkedHashMap<String, Metadata>(lst.size());

			for (Entry<String, Stat> entry : lst.entrySet())
				mdLst.put(entry.getKey(), tsRead(entry.getKey(), entry.getValue()));

			return mdLst;
		} catch (Exception e) {
			logger.error("Could not perform transactional timestamped read.", e);
			throw new HybrisException("Could not perform transactional timestamped read: " + e.getMessage(), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#list()
	 */
	public List<String> list() throws HybrisException {

		List<String> znodes;
		try {
			znodes = this.zkCli.getChildren().forPath(this.storageRoot);
		} catch (Exception e) {
			logger.error("Could not list the children of ZNode " + this.storageRoot, e);
			throw new HybrisException(e);
		}

		for (Iterator<String> it = znodes.iterator(); it.hasNext();) {
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#delete(java.lang.String,
	 * fr.eurecom.hybris.mds.Metadata, int)
	 */
	public void delete(String key, Metadata tombstone, int zkVersion) throws HybrisException {
		this.tsWrite(key, tombstone, zkVersion);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#getOrCreateIv()
	 */
	public synchronized byte[] getOrCreateIv() {

		String ivKey = this.storageRoot + "-iv";
		byte[] iv = null;
		try {
			this.zkCli.sync().forPath(ivKey);
			iv = this.zkCli.getData().forPath(ivKey);
		} catch (Exception e) {
		}

		if (iv == null) {
			iv = new byte[16];
			Utils.generateRandomBytes(iv);
			try {
				this.zkCli.create().forPath(ivKey, iv);
			} catch (Exception e) {
				logger.warn("Could not store the generated IV on ZooKeeper");
			}
		}
		return iv;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#getAll()
	 */
	public Map<String, Metadata> getAll() throws HybrisException {
		try {
			return this.getAllChildrenMetadata(this.storageRoot);
		} catch (Exception e) {
			logger.error("Could not get all the metadata from children ZNodes of " + this.gcRoot, e);
			throw new HybrisException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#emptyMetadataContainer()
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#shutdown()
	 */
	public void shutdown() {
		this.zkCli.close();
	}

	/*
	 * -------------------------------------- GC functions
	 */

	public void markStaleKey(final String key) {
		new Thread(new Runnable() {
			public void run() {
				String path = gcStaleDir + "/" + key;
				try {
					// create ZNode <root>-gc/stale/<key>
					ZkRmds.this.zkCli.create().forPath(path);
					logger.debug("GcMarker: marked {} as stale", path);
				} catch (KeeperException e) {
					if (e.code() != KeeperException.Code.NODEEXISTS)
						logger.warn("GcMarker: could not create stale node " + path, e);
				} catch (Exception e) {
					logger.warn("GcMarker: could not create stale node " + path, e);
				}
			}
		}).start();
	}

	public void markOrphanKey(final String key, final Timestamp ts, final List<Kvs> savedReplicas) {
		new Thread(new Runnable() {
			public void run() {
				// create ZNode <root>-gc/orphans/<KvsKey>
				String path = gcOrphansDir + "/" + Utils.getKvsKey(key, ts);
				byte[] value = new Metadata(ts, null, 0, savedReplicas, null).serialize();
				try {
					ZkRmds.this.zkCli.create().forPath(path, value);
					logger.debug("GcMarker: marked {} as orphan", path);
				} catch (KeeperException e) {
					if (e.code() != KeeperException.Code.NODEEXISTS)
						logger.warn("GcMarker: could not create orphan node " + path, e);
				} catch (Exception e) {
					logger.warn("GcMarker: could not create orphan node " + path, e);
				}
			}
		}).start();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#getOrphans()
	 */
	public Map<String, Metadata> getOrphans() throws HybrisException {
		try {
			return this.getAllChildrenMetadata(this.gcOrphansDir);
		} catch (Exception e) {
			logger.error("Could not get all the metadata from children ZNodes of " + this.gcOrphansDir, e);
			throw new HybrisException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#getStaleKeys()
	 */
	public List<String> getStaleKeys() throws HybrisException {

		try {
			return this.zkCli.getChildren().forPath(this.gcStaleDir);
		} catch (Exception e) {
			logger.error("Could not list the children of ZNode " + this.storageRoot, e);
			throw new HybrisException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#removeOrphanKeys(java.util.Set)
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#removeStaleKey(java.lang.String)
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see fr.eurecom.hybris.mds.Mds1#emptyStaleAndOrphansContainers()
	 */
	public void emptyStaleAndOrphansContainers() throws HybrisException {

		try {
			for (String path : new String[] { this.gcOrphansDir, this.gcStaleDir }) {
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

	/*
	 * -------------------------------------------------------------------------
	 * Private methods
	 * -------------------------------------------------------------------------
	 */

	/**
	 * Retrieve all Metadata objects associated to the children of a certain
	 * key.
	 * 
	 * @param key
	 * @return a Map<String, Metadata> of all metadata associated with the ZNode
	 *         children of key
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
	 * 
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