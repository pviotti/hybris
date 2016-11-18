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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.data.Stat;

import fr.eurecom.hybris.Hybris.HybrisWatcher;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.mds.Metadata.Timestamp;
import fr.eurecom.hybris.HybrisException;

/**
 * Reliable Metadata Store interface.
 * @author viotti
 */
public interface Rmds {
	
	public static final String ZOOKEEPER_ID = "zk";
	public static final String CONSUL_ID = "consul";
	
	/* Conventional integer marker to tell whether a 
	 * metadata key has to be created (rather than modified).
	 * 
	 * Used as Zookeeper setData API parameter, 
	 * it forces overwriting no matter which 
	 * znode version is currently written. 
	 */
	public static final int NONODE = -1;
	
	
	/**
	 * Timestamped write on metadata storage.
	 * @param key - the key
	 * @param md - the metadata to be written
	 * @param version - the existing metadata key version we expect; -1 when the metadata key does not exist
	 * @return boolean: true if a metadata key has been modified and stale old values need to be garbage-collected
	 *                  false otherwise (i.e. a new metadata key has been created)
	 * @throws HybrisException
	 */
	boolean tsWrite(String key, Metadata md, long version) throws HybrisException;

	/**
	 * XXX Transactional writes of several znodes on ZooKeeper.
	 * @param lst LinkedHashMap, sorted by insertion order map of key String and Metadata objects
	 * @param versions array of znode versions of the corresponding keys in lst 
	 * @return
	 * @throws HybrisException 
	 */
	boolean tsMultiWrite(LinkedHashMap<String, Metadata> mdMap, LinkedHashMap<String, Stat> statMap)
			throws HybrisException;

	/**
	 * Timestamped read ("slow read" in ZooKeeper parlance) from metadata storage.
	 * @param key the key to read
	 * @param stat the Stat ZooKeeper object to be written with znode details (can be null)
	 * @return Metadata object
	 *              or null in case the znode does not exist or there is a tombstone Metadata object
	 *              (to distinguish these two cases one must use the Stat object)
	 * @throws HybrisException
	 */
	Metadata tsRead(String key, Stat stat) throws HybrisException;

	/**
	 * XXX
	 * @param lst
	 * @return
	 * @throws HybrisException 
	 */
	LinkedHashMap<String, Metadata> tsMultiRead(LinkedHashMap<String, Stat> lst) throws HybrisException;

	/**
	 * Timestamped read ("slow read" in ZooKeeper parlance) from metadata storage.
	 * @param key the key to read
	 * @param stat the Stat Zookeeper object to be written with znode details (can be null)
	 * @param watcher to set upon executing the getData operation
	 * @return Metadata object
	 *              or null in case the znode does not exist or there is a tombstone Metadata object
	 *              (to distinguish these two cases one must use the Stat object)
	 * @throws HybrisException
	 */
	Metadata tsRead(String key, Stat stat, HybrisWatcher watcher) throws HybrisException;

	/**
	 * Get the list of metadata keys stored (filtering out tombstone values).
	 * @return the list of metadata keys stored in the
	 * @throws HybrisException
	 */
	List<String> list() throws HybrisException;

	/**
	 * Mark a key as deleted writing a tombstone value.
	 * @param key
	 * @param tombstone the tombstone metadata to be written
	 * @param version
	 * @throws HybrisException
	 */
	void delete(String key, Metadata tombstone, int version) throws HybrisException;

	byte[] getOrCreateIv();

	/**
	 * Get all the stored metadata (filtering out tombstone values).
	 * XXX not scalable - for debugging purposes
	 * @return a map of keys (String) and Metadata objects
	 * @throws HybrisException
	 */
	Map<String, Metadata> getAll() throws HybrisException;

	/**
	 * Empty the metadata storage root container.
	 * ATTENTION: it erases all metadata stored in the root container!
	 * @throws HybrisException
	 */
	void emptyMetadataContainer() throws HybrisException;

	void shutdown();

	/**
	 * Mark key as stale - after it has been overwritten on Kvs stores.
	 * @param key 
	 * @throws HybrisException
	 */
	void markStaleKey(String key);
	
	/**
	 * Mark key as orphan - due to uncompleted write operations.
	 * @param key 
	 * @param ts
	 * @param savedReplicas
	 * @throws HybrisException
	 */
	void markOrphanKey(String key, Timestamp ts, List<Kvs> savedReplicas);	
	
	/**
	 * Get orphan keys and their metadata.
	 * @return
	 * @throws HybrisException
	 */
	Map<String, Metadata> getOrphans() throws HybrisException;

	/**
	 * Get stale keys.
	 * @return
	 * @throws HybrisException
	 */
	List<String> getStaleKeys() throws HybrisException;

	/**
	 * Delete the set of orphan keys passed as argument.
	 * @param orphanKeys
	 */
	void removeOrphanKeys(Set<String> orphanKeys);

	/**
	 * Delete the key passed as argument from the list of keys
	 * to be checked as outdated for gc.
	 * @param staleKeys
	 */
	void removeStaleKey(String staleKey);

	/**
	 * Empty stale and orphan keys containers.
	 * @throws HybrisException
	 */
	void emptyStaleAndOrphansContainers() throws HybrisException;

}