package fr.eurecom.hybris.test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.kvs.CloudProvider;
import fr.eurecom.hybris.kvs.KvStore;


public class KvStoreTest extends HybrisAbstractTest {

    private static KvStore kvs;
    
    private static String KVS_ROOT = "kvstest-root-prova";
    
    @BeforeClass
    public static void beforeClassSetup() throws IOException {
        Config.getInstance();
        kvs = new KvStore("accounts-test.properties", KVS_ROOT, true);
    }
    
    // Executed before each test
    @Before
    public void setUp() throws Exception {
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders())
                kvs.emptyStorageContainer(provider);
        }
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
    
    @Test
    public void testPutGetDeleteList() throws IOException {
        
        String key1 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key2 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key3 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key4 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        byte[] value1 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".getBytes();
        byte[] value2 = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".getBytes();
        byte[] value3 = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC".getBytes();
        byte[] value4 = "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD".getBytes();
        
        List<CloudProvider> replicas = new ArrayList<CloudProvider>();
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders())
                try {
                    kvs.put(provider, key1, value1);
                    kvs.put(provider, key2, value2);
                    kvs.put(provider, key3, value3);
                    kvs.put(provider, key4, value4);
                    replicas.add(provider);
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
        assertTrue(replicas.size() > 0);
        
        List<String> keysLst;
        for(CloudProvider replica : replicas) {
            keysLst = kvs.list(replica);
            assertTrue(keysLst.size() > 3);
            assertTrue(keysLst.contains(key1));
            assertTrue(keysLst.contains(key2));
            assertTrue(keysLst.contains(key3));
            assertTrue(keysLst.contains(key4));
        }
        
        byte[] output;
        for(CloudProvider replica : replicas) {
            output = kvs.get(replica, key1);
            assertTrue(Arrays.equals(output, value1));
            output = kvs.get(replica, key2);
            assertTrue(Arrays.equals(output, value2));
            output = kvs.get(replica, key3);
            assertTrue(Arrays.equals(output, value3));
            output = kvs.get(replica, key4);
            assertTrue(Arrays.equals(output, value4));
        }
        
        for(CloudProvider replica : replicas){
            kvs.delete(replica, key1);
            kvs.delete(replica, key2);
            kvs.delete(replica, key3);
        }
        
        for(CloudProvider replica : replicas) {
            assertNull(kvs.get(replica, key1));
            assertNull(kvs.get(replica, key2));
            assertNull(kvs.get(replica, key3));
            assertNotNull(kvs.get(replica, key4));
        }
        
        for(CloudProvider replica : replicas) {
            keysLst = kvs.list(replica);
            assertFalse(keysLst.contains(key1));
            assertFalse(keysLst.contains(key2));
            assertFalse(keysLst.contains(key3));
            assertTrue(keysLst.contains(key4));
        }
    }
}
