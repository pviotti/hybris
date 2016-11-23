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
package fr.eurecom.hybris.kvs.drivers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transient in-memory key-value store - for testing purposes.
 * @author P. Viotti
 */
public class TransientKvs extends Kvs {

    private transient final Map<String, byte[]> hashMap;

    public TransientKvs(String id, String container, boolean enabled, int cost) {
        super(id, container, enabled, cost);
        this.hashMap = new ConcurrentHashMap<String, byte[]>();
    }

    public void put(String key, byte[] value) {
        this.hashMap.put(getOriginalKey(key), value);
    }

    public byte[] get(String key) {
        return this.hashMap.get(getOriginalKey(key));
    }

    public void delete(String key) {
        this.hashMap.remove(getOriginalKey(key));
    }

    public List<String> list() {
        return new ArrayList<String>(this.hashMap.keySet());
    }
    
    /*
     * To test this dummy key-value store using clients 
     * on different machines we need the keys stored in memory 
     * to be equal to the original (Hybris) one. 
     * Hence, this function strips the "#sn_cid" suffix.
     */
    private String getOriginalKey(String compositeKey) {
    	if (compositeKey.indexOf("#") != -1)
    		return compositeKey.substring(0, compositeKey.indexOf("#"));
    	else
    		return compositeKey;
    }
}
