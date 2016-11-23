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
 * Faulty key-value store - for testing purposes.
 * @author P. Viotti
 */
public class FaultyKvs extends Kvs {

    private transient final Map<String, byte[]> hashMap;

    public FaultyKvs(String id, String container, boolean enabled, int cost) {
        super(id, container, enabled, cost);
        this.hashMap = new ConcurrentHashMap<String, byte[]>();
    }

    public void put(String key, byte[] value) {
        this.hashMap.put(key, value);
    }

    // Faulty get API: returns bogus data.
    public byte[] get(String key) {
        return new byte[] { 0x01, 0x02, 0x03 };
    }

    public void delete(String key) {
        this.hashMap.remove(key);
    }

    public List<String> list() {
        return new ArrayList<String>(this.hashMap.keySet());
    }
}