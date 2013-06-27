package fr.eurecom.hybris.test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.eurecom.hybris.kvs.CloudProvider;
import fr.eurecom.hybris.kvs.KvStore;


public class KvStoreTest extends HybrisAbstractTest {

    private KvStore kvs;
    
    private String KVS_ROOT = "kvstest-root";
    
    @Before
    public void setUp() throws Exception {  
        kvs = new KvStore(KVS_ROOT, false);
    }

    @After
    public void tearDown() throws Exception { }

    @Test
    public void testBasicWriteAndRead() throws IOException {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        byte[] value = (new BigInteger(50, random).toString(32)).getBytes();
        
        List<CloudProvider> replicas = new ArrayList<CloudProvider>();
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders())
                try {
                    kvs.put(provider, key, value);                    
                    replicas.add(provider);
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
        assertTrue(replicas.size() > 0);
        
        for(CloudProvider replica : replicas) {
            byte[] output = kvs.get(replica, key);
            assertTrue(Arrays.equals(output, value));
        }
        
        for(CloudProvider replica : replicas)
            kvs.delete(replica, key);
        
        for(CloudProvider replica : replicas)
            assertNull(kvs.get(replica, key));
    }
}
