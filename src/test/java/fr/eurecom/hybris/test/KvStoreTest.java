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
    
    @Test
    public void testOverwrite() {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        byte[] value1 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".getBytes();
        byte[] value2 = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".getBytes();
        
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders())
                try {
                    kvs.put(provider, key, value1);
                    kvs.put(provider, key, value2);
                    
                    byte[] retrievedValue = kvs.get(provider, key);
                    assertTrue(Arrays.equals(value2, retrievedValue));
                    assertFalse(Arrays.equals(value1, retrievedValue));
                    
                    kvs.delete(provider, key);
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }
    
    @Test
    public void testDeleteNotExistingKey() {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders())
                try {
                    kvs.delete(provider, key);
                } catch(Throwable t) {
                    t.printStackTrace();
                    fail();
                }
        }
    }
    
    @Test
    public void testReadNotExistingKey() {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        byte[] value = null;
        
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders())
                try {
                    value = kvs.get(provider, key);
                    assertNull(value);
                } catch(Throwable t) {
                    t.printStackTrace();
                    fail();
                }
        }
    }
}
