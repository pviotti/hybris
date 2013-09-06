package fr.eurecom.hybris.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
import fr.eurecom.hybris.kvs.KvsManager;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.mds.MdsManager;
import fr.eurecom.hybris.mds.MdsManager.GcMarker;
import fr.eurecom.hybris.mds.Metadata.Timestamp;

@Ignore
public class HybrisGcTest extends HybrisAbstractTest {

    private final Hybris hybris;
    private final KvsManager kvs;
    private final MdsManager mds;

    private final String MDS_TEST_ROOT = "mdstest-root";
    private final String MDS_ADDRESS = "localhost:2181";

    private final String KVS_ROOT = "kvstest-root";
    private final String KVS_ACCOUNTS_FILE = "accounts.properties";

    public HybrisGcTest() throws HybrisException, IOException {
        this.hybris = new Hybris(this.MDS_ADDRESS, this.MDS_TEST_ROOT, this.KVS_ACCOUNTS_FILE,
                this.KVS_ROOT, false, 0, 600, 600, true);

        this.mds = new MdsManager(this.MDS_ADDRESS, this.MDS_TEST_ROOT);
        this.kvs = new KvsManager(this.KVS_ACCOUNTS_FILE, this.KVS_ROOT, false);
    }

    // Executed before each test
    @Before
    public void setUp() throws Exception {
        this.mds.emptyMetadataContainer();
        this.mds.emptyStaleAndOrphansContainers();
        for (Kvs provider : this.kvs.getKvStores())
            try{
                this.kvs.emptyStorageContainer(provider);
            } catch (Exception e) {}
    }

    @Test
    public void testGcKey() throws HybrisException, IOException {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value1 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value2 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value3 = new BigInteger(50, this.random).toString(32).getBytes();

        // write 3 different versions of the same key and then gc
        this.hybris.write(key, value1);
        this.hybris.write(key, value2);
        this.hybris.write(key, value3);

        this.hybris.gc(key);

        // check that the outdated versions are gone
        for (Kvs provider : this.kvs.getKvStores()) {
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key, new Timestamp(0, Utils.getClientId()))));
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key, new Timestamp(1, Utils.getClientId()))));
        }

        // check that right versions are still there
        byte[] retrieved = this.hybris.read(key);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value3, retrieved));
    }

    @Test
    public void testGc() throws HybrisException, IOException, InterruptedException {

        String key1 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key2 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key3 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value1 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value2 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value3 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value4 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value5 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value6 = new BigInteger(50, this.random).toString(32).getBytes();

        // populate stales
        this.hybris.write(key1, value1);
        this.hybris.write(key1, value2);
        this.hybris.write(key1, value3);

        this.hybris.write(key2, value4);
        this.hybris.write(key2, value5);

        // populate orphans
        Timestamp ts = new Timestamp(1, Utils.getClientId());
        List<Kvs> savedReplicas = new ArrayList<Kvs>();
        savedReplicas.add(this.kvs.getKvStores().get(0));
        savedReplicas.add(this.kvs.getKvStores().get(1));
        this.kvs.put(savedReplicas.get(0), Utils.getKvsKey(key3, ts), value6);
        this.kvs.put(savedReplicas.get(1), Utils.getKvsKey(key3, ts), value6);

        GcMarker gcm = this.mds.new GcMarker(key3, ts, savedReplicas);
        gcm.start();
        gcm.join();

        // gc
        this.hybris.gc();

        // check that stale keys and orphans are gone
        for (Kvs provider : this.kvs.getKvStores()) {
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key1, new Timestamp(0, Utils.getClientId()))));
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key1, new Timestamp(1, Utils.getClientId()))));

            assertNull(this.kvs.get(provider, Utils.getKvsKey(key2, new Timestamp(0, Utils.getClientId()))));
        }

        for (Kvs provider : savedReplicas)
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key3, ts)));

        // check that right versions are still there
        byte[] retrieved = this.hybris.read(key1);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value3, retrieved));

        retrieved = this.hybris.read(key2);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value5, retrieved));

        retrieved = this.hybris.read(key3);
        assertNull(retrieved);
    }

    @Test
    public void testBatchGc() throws HybrisException, IOException, InterruptedException {

        // write random  stuff on kvs
        this.kvs.put(this.kvs.getKvStores().get(0), "malformedkey1", "fakevalue1".getBytes());
        this.kvs.put(this.kvs.getKvStores().get(1), "malformedkey2", "fakevalue2".getBytes());

        // write a couple of stales
        String key1 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key2 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key3 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value1 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value2 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value3 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value4 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value5 = new BigInteger(50, this.random).toString(32).getBytes();
        byte[] value6 = new BigInteger(50, this.random).toString(32).getBytes();

        // populate stales
        this.hybris.write(key1, value1);
        this.hybris.write(key1, value2);
        this.hybris.write(key1, value3);

        this.hybris.write(key2, value4);
        this.hybris.write(key2, value5);

        // make up a couple of orphans
        Timestamp ts = new Timestamp(1, Utils.getClientId());
        List<Kvs> savedReplicas = new ArrayList<Kvs>();
        savedReplicas.add(this.kvs.getKvStores().get(0));
        savedReplicas.add(this.kvs.getKvStores().get(1));
        this.kvs.put(savedReplicas.get(0), Utils.getKvsKey(key3, ts), value6);
        this.kvs.put(savedReplicas.get(1), Utils.getKvsKey(key3, ts), value6);

        GcMarker gcm = this.mds.new GcMarker(key3, ts, savedReplicas);
        gcm.start();
        gcm.join();

        // batchGc
        this.hybris.batchGc();

        // make sure stales and orphans are gone
        assertNull(this.kvs.get(this.kvs.getKvStores().get(0), "malformedkey1"));
        assertNull(this.kvs.get(this.kvs.getKvStores().get(1), "malformedkey2"));

        for (Kvs provider : this.kvs.getKvStores()) {
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key1, new Timestamp(0, Utils.getClientId()))));
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key1, new Timestamp(1, Utils.getClientId()))));

            assertNull(this.kvs.get(provider, Utils.getKvsKey(key2, new Timestamp(0, Utils.getClientId()))));
        }

        for (Kvs provider : savedReplicas)
            assertNull(this.kvs.get(provider, Utils.getKvsKey(key3, ts)));

        // check that right versions are still there
        byte[] retrieved = this.hybris.read(key1);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value3, retrieved));

        retrieved = this.hybris.read(key2);
        assertNotNull(retrieved);
        assertTrue(Arrays.equals(value5, retrieved));

        retrieved = this.hybris.read(key3);
        assertNull(retrieved);
    }
}