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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.v1.ConsistencyMode;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;
import com.ecwid.consul.v1.kv.model.PutParams;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.Hybris.HybrisWatcher;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.mds.Metadata.Timestamp;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.Utils;

/**
 * Reliable Metadata Store based on Consul.
 * 
 * @author P. Viotti
 */
public class ConsulRmds implements Rmds {

	private static final Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

	private final ConsulClient cClient;
	private final String storageRoot;

    private final String gcRoot;
    private final String gcStaleDir;
    private final String gcOrphansDir;
    
	public ConsulRmds(String connectionStr, String root, boolean qRead) {
		storageRoot = root;
		gcRoot = this.storageRoot + "-gc";
        gcStaleDir = this.gcRoot + "/stale";
        gcOrphansDir = this.gcRoot + "/orphans";
        
		cClient = new ConsulClient(connectionStr);
		cClient.setKVBinaryValue(storageRoot, new byte[1]);
	}

	public boolean tsWrite(String key, Metadata md, long version) throws HybrisException {

		String path = this.storageRoot + "/" + key;
		Response<Boolean> res;
		PutParams pp = new PutParams();

		if (version == NONODE) {
			pp.setCas(0L); // only put if does not already exist
			res = cClient.setKVBinaryValue(path, md.serialize(), pp);
			if (res.getValue())
				logger.debug("Md {} created.", path);
		} else {
			pp.setCas(version);
			res = cClient.setKVBinaryValue(path, md.serialize(), pp);
			if (res.getValue())
				logger.debug("Md {} modified.", path);
		}

		if (!res.getValue()) {
			QueryParams qp = new QueryParams(ConsistencyMode.CONSISTENT);
			Response<GetBinaryValue> resGet = cClient.getKVBinaryValue(path, qp);
			if (resGet.getValue() == null)
				throw new HybrisException("Could not write key " + key + ": not found");
			Metadata newmd = new Metadata(resGet.getValue().getValue());

			if (md.getTs().isGreater(newmd.getTs())) {
				logger.debug("Found smaller version ({}) writing {}: retrying.", newmd.getTs(), key);
				return this.tsWrite(key, md, resGet.getConsulIndex());
			} else {
				logger.warn("Found greater version ({}) writing {}: failing.", newmd.getTs(), key);
				return false;
			}
		} else
			return version != NONODE;
	}

	public Metadata tsRead(String key, Stat stat) throws HybrisException {
		String path = this.storageRoot + "/" + key;
		QueryParams qp = new QueryParams(ConsistencyMode.CONSISTENT);
		Response<GetBinaryValue> resGet = cClient.getKVBinaryValue(path, qp);

		if (resGet.getValue() == null)
			return null;
		else {
			if (stat != null) // Math.toIntExact throws an exception if the value overflows an int
				stat.setVersion(Math.toIntExact(resGet.getConsulIndex()));
			return new Metadata(resGet.getValue().getValue());
		}
	}

	public boolean tsMultiWrite(LinkedHashMap<String, Metadata> mdMap, LinkedHashMap<String, Stat> statMap)
			throws HybrisException {
		// XXX not implemented - available only for Consul > 0.7
		return false;
	}

	public LinkedHashMap<String, Metadata> tsMultiRead(LinkedHashMap<String, Stat> lst) throws HybrisException {
		// XXX not implemented - available only for Consul > 0.7
		return null;
	}

	public Metadata tsRead(String key, Stat stat, HybrisWatcher watcher) throws HybrisException {
		// XXX implement support for watches (not present in adopted client lib)
		// present in alternative library https://github.com/OrbitzWorldwide/consul-client/issues/31
		return tsRead(key, stat);
	}

	public List<String> list() throws HybrisException {
		Response<List<String>> lstResp = cClient.getKVKeysOnly(storageRoot);
		List<String> lstKeys = new LinkedList<String>();
		for (String key : lstResp.getValue()) {
			QueryParams qp = new QueryParams(ConsistencyMode.CONSISTENT);
			Response<GetBinaryValue> resGet = cClient.getKVBinaryValue(key, qp);
			if (!key.equals(storageRoot) && !(new Metadata(resGet.getValue().getValue())).isTombstone())
				lstKeys.add(key.split("/")[1]);

		}
		return lstKeys;
	}

	public void delete(String key, Metadata tombstone, int version) throws HybrisException {
		this.tsWrite(key, tombstone, version);
	}

	public byte[] getOrCreateIv() {
		String ivKey = this.storageRoot + "-iv";
		byte[] iv = null;
		QueryParams qp = new QueryParams(ConsistencyMode.CONSISTENT);
		Response<GetBinaryValue> resGet = cClient.getKVBinaryValue(ivKey, qp);
		if (resGet.getValue() == null) {
			iv = new byte[16];
			Utils.generateRandomBytes(iv);
			cClient.setKVBinaryValue(ivKey, iv);
		} else
			iv = resGet.getValue().getValue();
		return iv;
	}

	public Map<String, Metadata> getAll() throws HybrisException {
		List<String> keys = list();
		System.out.println(keys);

		HashMap<String, Metadata> retMap = new HashMap<String, Metadata>();
		for (String key : keys) {
			Metadata md = tsRead(key, null);
			if (md != null && !md.isTombstone())
				retMap.put(key, md);
		}
		return retMap;
	}

	public void emptyMetadataContainer() throws HybrisException {
		Response<List<String>> keys = cClient.getKVKeysOnly(storageRoot);
		keys.getValue().remove(0); // remove the root itself
		for (String key : keys.getValue())
			cClient.deleteKVValue(key);
	}

	public void shutdown() {
	}

	/* -------------------------------------- GC functions */
	// XXX not implemented
	
	public void markStaleKey(final String key) {
		new Thread(new Runnable() {
			public void run() {
				// create ZNode  <root>-gc/stale/<key>
                String path = ConsulRmds.this.gcStaleDir + "/" + key;
                try {
                	ConsulRmds.this.cClient.setKVBinaryValue(path, Metadata.getTombstone(new Timestamp(0, "gc")).serialize());
                    logger.debug("GcMarker: marked {} as stale", path);
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
                String path = ConsulRmds.this.gcOrphansDir + "/" + Utils.getKvsKey(key, ts);
                byte[] value = new Metadata(ts, null, 0, savedReplicas, null).serialize();
                try {
                	ConsulRmds.this.cClient.setKVBinaryValue(path, value);
                    logger.debug("GcMarker: marked {} as orphan", path);
                } catch (Exception e) {
                    logger.warn("GcMarker: could not create orphan node " + path, e);
                }
			}
		}).start();
	} 
	

	public Map<String, Metadata> getOrphans() throws HybrisException {
		return null;
	}

	public List<String> getStaleKeys() throws HybrisException {
		return null;
	}

	public void removeOrphanKeys(Set<String> orphanKeys) {
	}

	public void removeStaleKey(String staleKey) {
	}

	public void emptyStaleAndOrphansContainers() throws HybrisException {
	}

	public Map<String, Metadata> getAllChildrenMetadata(String key) throws Exception {
		return null;
	}
	
}