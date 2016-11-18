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
package fr.eurecom.hybris;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.kvs.KvsManager;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.mds.Metadata;
import fr.eurecom.hybris.mds.Metadata.Timestamp;
import fr.eurecom.hybris.mds.Rmds;

/**
 * Class in charge of performing garbage collection tasks.
 * 
 * @author P. Viotti
 */
public class GcManager {

	private static final Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

	private final KvsManager kvs;
	private final Rmds mds;

	public GcManager(Hybris hybris) {
		this.kvs = hybris.kvs;
		this.mds = hybris.mds;
	}

	/**
	 * Deletes from KVSs all orphan or stale keys which are indexed on MDS.
	 * 
	 * @throws HybrisException
	 */
	public void gc() throws HybrisException {

		// Orphans
		Map<String, Metadata> orphans = mds.getOrphans();
		orphans.keySet().forEach(kvsKey -> {
			Metadata md = orphans.get(kvsKey);
			boolean error = false;

			for (Kvs kvStore : kvs.getKvsList()) {

				if (!md.getReplicasLst().contains(kvStore))
					continue;

				try {
					kvs.delete(kvStore, kvsKey);
				} catch (IOException e) {
					error = true;
					logger.warn("GC: could not delete {} from {}", kvsKey, kvStore);
				}
			}

			if (error)
				orphans.remove(kvsKey);
		});
		mds.removeOrphanKeys(orphans.keySet());

		// Stale
		List<String> staleKeys = mds.getStaleKeys();
		for (String key : staleKeys)
			try {
				this.gc(key);
			} catch (HybrisException e) {
				logger.warn("GC: could not gc key {}", key);
			}
	}

	/**
	 * Deletes from KVSs stale data associated with <key>.
	 * 
	 * @param key
	 * @throws HybrisException
	 */
	public void gc(String key) throws HybrisException {

		Metadata md = mds.tsRead(key, null);
		if (md == null) {
			logger.debug("GC: could not find the metadata associated with key {}.", key);
			return;
		}

		for (Kvs kvStore : kvs.getKvsList()) {

			List<String> kvsKeys;
			try {
				kvsKeys = kvs.list(kvStore);
			} catch (IOException e) {
				logger.warn("GC: could not list {} container", kvStore);
				continue;
			}

			for (String kvsKey : kvsKeys) {
				String prefixKey = "";
				Timestamp kvTs = null;
				boolean malformedKey = false;
				try {
					prefixKey = Utils.getKeyFromKvsKey(kvsKey);
					kvTs = Utils.getTimestampfromKvsKey(kvsKey);
				} catch (IndexOutOfBoundsException e) {
					malformedKey = true;
				}

				if (malformedKey || key.equals(prefixKey) && md.getTs().isGreater(kvTs)) {
					try {
						kvs.delete(kvStore, kvsKey);
					} catch (IOException e) {
						logger.warn("GC: could not delete {} from {}", kvsKey, kvStore);
						continue;
					}
					logger.debug("GC: deleted {} from {}", kvsKey, kvStore);
				}
			}
		}

		mds.removeStaleKey(key);
	}

	/**
	 * Deletes from KVSs all the keys which are not present on MDS or obsolete
	 * or malformed. Heads up: this function does a complete MDS dump and a
	 * complete KVS listing, so it can be very slow and resource consuming.
	 * 
	 * @throws HybrisException
	 */
	public void batchGc() throws HybrisException {

		Map<String, Metadata> mdMap = mds.getAll(); // !! heavy operation

		for (Kvs kvStore : kvs.getKvsList()) {

			List<String> kvsKeys;
			try {
				kvsKeys = kvs.list(kvStore);
			} catch (IOException e) {
				logger.warn("GC: could not list {} container", kvStore);
				continue;
			}

			for (String kvsKey : kvsKeys) {

				String key = "";
				Timestamp kvTs = null;
				boolean malformedKey = false;
				try {
					key = Utils.getKeyFromKvsKey(kvsKey);
					kvTs = Utils.getTimestampfromKvsKey(kvsKey);
				} catch (IndexOutOfBoundsException e) {
					malformedKey = true;
				}

				if (malformedKey || !mdMap.keySet().contains(key) || mdMap.get(key).getTs().isGreater(kvTs)) {
					try {
						kvs.delete(kvStore, kvsKey);
					} catch (IOException e) {
						logger.warn("GC: could not delete {} from {}", kvsKey, kvStore);
						continue;
					}
					logger.debug("GC: deleted {} from {}", kvsKey, kvStore);
				}
			}
		}
		// mds.emptyStaleAndOrphansContainers();
	}

	/*
	 * TEMP / DEBUG --------------------------------------
	 */
	
	/**
	 * TEMP for testing and debugging 
	 * cleans up the KVS and MDS containers  (!)
	 * 
	 * @throws HybrisException
	 */
	public void _emptyContainers() throws HybrisException {
		mds.emptyMetadataContainer();
		for (Kvs kvStore : kvs.getKvsList())
			try {
				kvs.emptyStorageContainer(kvStore);
			} catch (IOException e) {
				logger.warn("Could not empty {} container", kvStore);
			}
	}
}
