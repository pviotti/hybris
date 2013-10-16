package fr.eurecom.hybris.test.kvs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.kvs.KvsManager;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.test.HybrisAbstractTest;


public class KvsManagerTest extends HybrisAbstractTest {

    private static KvsManager kvs;

    private static String KVS_ROOT = "kvstest-root-prova";

    @BeforeClass
    public static void beforeClassSetup() throws IOException {
        Config.getInstance();
        kvs = new KvsManager("accounts-test.properties", KVS_ROOT, true);
    }

    // Executed before each test
    @Before
    public void setUp() throws Exception {
        for (Kvs provider : kvs.getKvsList())
            kvs.emptyStorageContainer(provider);
    }

    @After
    public void tearDown() throws Exception { }

    @Test
    public void testBasicWriteAndRead() throws IOException {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value = new BigInteger(50, this.random).toString(32).getBytes();

        List<Kvs> replicas = new ArrayList<Kvs>();
        for (Kvs provider : kvs.getKvsList())
            try {
                kvs.put(provider, key, value);
                replicas.add(provider);
            } catch (Exception e) {
                e.printStackTrace();
            }
        assertTrue(replicas.size() > 0);

        for(Kvs replica : replicas) {
            byte[] output = kvs.get(replica, key);
            assertTrue(Arrays.equals(output, value));
        }

        for(Kvs replica : replicas)
            kvs.delete(replica, key);

        for(Kvs replica : replicas)
            assertNull(kvs.get(replica, key));
    }

    @Test
    public void testOverwrite() {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value1 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".getBytes();
        byte[] value2 = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".getBytes();

        for (Kvs provider : kvs.getKvsList())
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

    @Test
    public void testDeleteNotExistingKey() {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);

        for (Kvs provider : kvs.getKvsList())
            try {
                kvs.delete(provider, key);
            } catch(Throwable t) {
                t.printStackTrace();
                fail();
            }
    }

    @Test
    public void testReadNotExistingKey() {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value = null;

        for (Kvs provider : kvs.getKvsList())
            try {
                value = kvs.get(provider, key);
                assertNull(value);
            } catch(Throwable t) {
                t.printStackTrace();
                fail();
            }
    }

    @Test
    public void testPutGetDeleteList() throws IOException {

        String key1 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key2 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key3 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key4 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value1 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".getBytes();
        byte[] value2 = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".getBytes();
        byte[] value3 = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC".getBytes();
        byte[] value4 = "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD".getBytes();

        List<Kvs> replicas = new ArrayList<Kvs>();
        for (Kvs provider : kvs.getKvsList())
            try {
                kvs.put(provider, key1, value1);
                kvs.put(provider, key2, value2);
                kvs.put(provider, key3, value3);
                kvs.put(provider, key4, value4);
                replicas.add(provider);
            } catch (Exception e) {
                e.printStackTrace();
            }
        assertTrue(replicas.size() > 0);

        List<String> keysLst;
        for(Kvs replica : replicas) {
            keysLst = kvs.list(replica);
            assertTrue(keysLst.size() > 3);
            assertTrue(keysLst.contains(key1));
            assertTrue(keysLst.contains(key2));
            assertTrue(keysLst.contains(key3));
            assertTrue(keysLst.contains(key4));
        }

        byte[] output;
        for(Kvs replica : replicas) {
            output = kvs.get(replica, key1);
            assertTrue(Arrays.equals(output, value1));
            output = kvs.get(replica, key2);
            assertTrue(Arrays.equals(output, value2));
            output = kvs.get(replica, key3);
            assertTrue(Arrays.equals(output, value3));
            output = kvs.get(replica, key4);
            assertTrue(Arrays.equals(output, value4));
        }

        for(Kvs replica : replicas){
            kvs.delete(replica, key1);
            kvs.delete(replica, key2);
            kvs.delete(replica, key3);
        }

        for(Kvs replica : replicas) {
            assertNull(kvs.get(replica, key1));
            assertNull(kvs.get(replica, key2));
            assertNull(kvs.get(replica, key3));
            assertNotNull(kvs.get(replica, key4));
        }

        for(Kvs replica : replicas) {
            keysLst = kvs.list(replica);
            assertFalse(keysLst.contains(key1));
            assertFalse(keysLst.contains(key2));
            assertFalse(keysLst.contains(key3));
            assertTrue(keysLst.contains(key4));
        }
    }

    @Ignore
    @Test
    public void testLargeFiles() throws IOException {
        byte[] payload = this.generatePayload(41940000, (byte) 'x');
        String key1 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);

        List<Kvs> replicas = new ArrayList<Kvs>();
        for (Kvs provider : kvs.getKvsList()) {
            kvs.put(provider, key1, payload);
            replicas.add(provider);
        }

        for(Kvs replica : replicas)
            kvs.delete(replica, key1);
    }
}
