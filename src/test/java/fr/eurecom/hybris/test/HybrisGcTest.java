package fr.eurecom.hybris.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.CloudProvider;
import fr.eurecom.hybris.kvs.KvStore;
import fr.eurecom.hybris.mds.MdStore;
import fr.eurecom.hybris.mds.MdStore.GcMarker;
import fr.eurecom.hybris.mds.Metadata.Timestamp;

@Ignore
public class HybrisGcTest extends HybrisAbstractTest {

    private Hybris hybris;
    private KvStore kvs;
    private MdStore mds;
    
    private String MDS_TEST_ROOT = "mdstest-root";
    private String MDS_ADDRESS = "localhost:2181";
    
    private String KVS_ROOT = "kvstest-root";
    private String KVS_ACCOUNTS_FILE = "accounts.properties";
    
    public HybrisGcTest() throws HybrisException, IOException {
        hybris = new Hybris(MDS_ADDRESS, MDS_TEST_ROOT, KVS_ACCOUNTS_FILE,
                                KVS_ROOT, false, 0, 600, 600, true);
        
        mds = new MdStore(MDS_ADDRESS, MDS_TEST_ROOT);
        kvs = new KvStore(KVS_ACCOUNTS_FILE, KVS_ROOT, false);
    }
    
    // Executed before each test
    @Before
    public void setUp() throws Exception {
        mds.emptyMetadataContainer();
        mds.emptyStaleAndOrphansContainers();
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders())
                try{
                    kvs.emptyStorageContainer(provider);
                } catch (Exception e) {}
        }
    }

    @Test
    public void testGcKey() throws HybrisException, IOException {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        byte[] value1 = (new BigInteger(50, random).toString(32)).getBytes();
        byte[] value2 = (new BigInteger(50, random).toString(32)).getBytes();
        byte[] value3 = (new BigInteger(50, random).toString(32)).getBytes();
        
        // write 3 different versions of the same key and then gc
        hybris.write(key, value1);
        hybris.write(key, value2);
        hybris.write(key, value3);
        
        hybris.gc(key);
        
        // check that the outdated versions are gone
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders()) {
                assertNull(kvs.get(provider, Utils.getKvsKey(key, new Timestamp(0, Utils.getClientId()))));
                assertNull(kvs.get(provider, Utils.getKvsKey(key, new Timestamp(1, Utils.getClientId()))));
            }
        }
        
        // check that right versions are still there
        byte[] retrieved = hybris.read(key);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value3, retrieved));    
    }
    
    @Test
    public void testGc() throws HybrisException, IOException, InterruptedException {
        
        String key1 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key2 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key3 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        byte[] value1 = (new BigInteger(50, random).toString(32)).getBytes();
        byte[] value2 = (new BigInteger(50, random).toString(32)).getBytes();
        byte[] value3 = (new BigInteger(50, random).toString(32)).getBytes();
        byte[] value4 = (new BigInteger(50, random).toString(32)).getBytes();
        byte[] value5 = (new BigInteger(50, random).toString(32)).getBytes();
        byte[] value6 = (new BigInteger(50, random).toString(32)).getBytes();
        
        // populate stales
        hybris.write(key1, value1);
        hybris.write(key1, value2);
        hybris.write(key1, value3);
        
        hybris.write(key2, value4);
        hybris.write(key2, value5);     
        
        // populate orphans
        Timestamp ts = new Timestamp(1, Utils.getClientId());
        List<CloudProvider> savedReplicas = new ArrayList<CloudProvider>();
        savedReplicas.add(kvs.getProviders().get(0));
        savedReplicas.add(kvs.getProviders().get(1));
        kvs.put(savedReplicas.get(0), Utils.getKvsKey(key3, ts), value6);
        kvs.put(savedReplicas.get(1), Utils.getKvsKey(key3, ts), value6);
        
        GcMarker gcm = mds.new GcMarker(key3, ts, savedReplicas);
        gcm.start();
        gcm.join();
        
        // gc
        hybris.gc();
        
        // check that stale keys and orphans are gone
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders()) {
                assertNull(kvs.get(provider, Utils.getKvsKey(key1, new Timestamp(0, Utils.getClientId()))));
                assertNull(kvs.get(provider, Utils.getKvsKey(key1, new Timestamp(1, Utils.getClientId()))));
                
                assertNull(kvs.get(provider, Utils.getKvsKey(key2, new Timestamp(0, Utils.getClientId()))));
            }
        }
        
        for (CloudProvider provider : savedReplicas)
            assertNull(kvs.get(provider, Utils.getKvsKey(key3, ts)));
        
        // check that right versions are still there
        byte[] retrieved = hybris.read(key1);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value3, retrieved));
        
        retrieved = hybris.read(key2);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value5, retrieved));
        
        retrieved = hybris.read(key3);
        assertNull(retrieved);
    }
    
    @Test
    public void testBatchGc() throws HybrisException, IOException, InterruptedException {
        
        // write random  stuff on kvs
        kvs.put(kvs.getProviders().get(0), "malformedkey1", "fakevalue1".getBytes());
        kvs.put(kvs.getProviders().get(1), "malformedkey2", "fakevalue2".getBytes());
        
        // write a couple of stales
        String key1 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key2 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        String key3 = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        byte[] value1 = (new BigInteger(50, random).toString(32)).getBytes();
        byte[] value2 = (new BigInteger(50, random).toString(32)).getBytes();
        byte[] value3 = (new BigInteger(50, random).toString(32)).getBytes();
        byte[] value4 = (new BigInteger(50, random).toString(32)).getBytes();
        byte[] value5 = (new BigInteger(50, random).toString(32)).getBytes();
        byte[] value6 = (new BigInteger(50, random).toString(32)).getBytes();
        
        // populate stales
        hybris.write(key1, value1);
        hybris.write(key1, value2);
        hybris.write(key1, value3);
        
        hybris.write(key2, value4);
        hybris.write(key2, value5);     
        
        // make up a couple of orphans
        Timestamp ts = new Timestamp(1, Utils.getClientId());
        List<CloudProvider> savedReplicas = new ArrayList<CloudProvider>();
        savedReplicas.add(kvs.getProviders().get(0));
        savedReplicas.add(kvs.getProviders().get(1));
        kvs.put(savedReplicas.get(0), Utils.getKvsKey(key3, ts), value6);
        kvs.put(savedReplicas.get(1), Utils.getKvsKey(key3, ts), value6);
        
        GcMarker gcm = mds.new GcMarker(key3, ts, savedReplicas);
        gcm.start();
        gcm.join();
        
        // batchGc
        hybris.batchGc();
        
        // make sure stales and orphans are gone
        assertNull(kvs.get(kvs.getProviders().get(0), "malformedkey1"));
        assertNull(kvs.get(kvs.getProviders().get(1), "malformedkey2"));
        
        synchronized(kvs.getProviders()) {
            for (CloudProvider provider : kvs.getProviders()) {
                assertNull(kvs.get(provider, Utils.getKvsKey(key1, new Timestamp(0, Utils.getClientId()))));
                assertNull(kvs.get(provider, Utils.getKvsKey(key1, new Timestamp(1, Utils.getClientId()))));
                
                assertNull(kvs.get(provider, Utils.getKvsKey(key2, new Timestamp(0, Utils.getClientId()))));
            }
        }
        
        for (CloudProvider provider : savedReplicas)
            assertNull(kvs.get(provider, Utils.getKvsKey(key3, ts)));
        
        // check that right versions are still there
        byte[] retrieved = hybris.read(key1);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value3, retrieved));
        
        retrieved = hybris.read(key2);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value5, retrieved));
        
        retrieved = hybris.read(key3);
        assertNull(retrieved);
    }
}