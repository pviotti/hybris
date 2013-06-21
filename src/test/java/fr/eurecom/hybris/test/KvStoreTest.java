package fr.eurecom.hybris.test;

import java.util.List;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.eurecom.hybris.kvs.KvStore;


public class KvStoreTest extends TestCase {

    private String BASIC_KEY = "testkey";
    private String BASIC_VALUE = "QWERTYUIOPLKJHGFDSAZXCVBNM";
    
    private String KVS_ROOT = "kvstest-root";
    
    private KvStore kvs;
    
    @Before
    public void setUp() throws Exception {  
        kvs = new KvStore(KVS_ROOT);
    }

    @After
    public void tearDown() throws Exception {  }

    @Test
    public void testBasicWriteAndRead() {
        
        List<String> replicas = kvs.put(BASIC_KEY, BASIC_VALUE.getBytes());
        assertTrue(replicas.size() > 0);
        
        for(String provider : replicas) {
            byte[] value = kvs.getFromCloud(provider, BASIC_KEY);
            assertEquals(BASIC_VALUE, new String(value));
        }
    }
}
