package fr.eurecom.hybris.test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        
        List<String> replicas = kvs.put(key, value);
        assertTrue(replicas.size() > 0);
        
        for(String provider : replicas) {
            byte[] output = kvs.getFromCloud(provider, key);
            assertTrue(Arrays.equals(output, value));
        }
        
        for(String provider : replicas)
            kvs.deleteKeyFromCloud(provider, key);
        
        for(String provider : replicas)
            assertNull(kvs.getFromCloud(provider, key));
    }
}
