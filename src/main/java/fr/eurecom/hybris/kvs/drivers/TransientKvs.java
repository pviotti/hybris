package fr.eurecom.hybris.kvs.drivers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransientKvs extends Kvs {

    private static final long serialVersionUID = 1L;
    private transient final Map<String, byte[]> hashMap;

    public TransientKvs(String id, String accessKey, String secretKey,
                            String container, boolean enabled, int cost) {
        super(id, accessKey, secretKey, container, enabled, cost);
        this.hashMap = new HashMap<String, byte[]>();
    }

    public void put(String key, byte[] value) {
        this.hashMap.put(key, value);
    }

    public byte[] get(String key) {
        return this.hashMap.get(key);
    }

    public void delete(String key) {
        this.hashMap.remove(key);
    }

    public List<String> list() {
        return new ArrayList<String>(this.hashMap.keySet());
    }

    public void createContainer() { }
}