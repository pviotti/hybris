package fr.eurecom.hybris.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.kvs.drivers.TransientKvs;
import fr.eurecom.hybris.mds.MdsManager;
import fr.eurecom.hybris.mds.Metadata;
import fr.eurecom.hybris.mds.Metadata.Timestamp;


public class MdsManagerTest extends HybrisAbstractTest {

    private static MdsManager mds;

    private static String MDS_TEST_ROOT = "mdstest-root";
    private static String MDS_ADDRESS = "localhost:2181";

    @BeforeClass
    public static void beforeClassSetup() throws IOException {
        Config.getInstance();
        mds = new MdsManager(MDS_ADDRESS, MDS_TEST_ROOT);
    }

    // Executed before each test
    @Before
    public void setUp() throws Exception {
        mds.emptyMetadataContainer();
        mds.emptyStaleAndOrphansContainers();
    }

    @After
    public void tearDown() throws Exception {  }

    @Test
    public void testBasicWriteAndRead() throws HybrisException {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        Timestamp ts = new Timestamp(new BigInteger(10, this.random).intValue(), Utils.getClientId());
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "A-accessKey", "A-secretKey", "container", true, 20));
        replicas.add(new TransientKvs("transient", "B-accessKey", "B-secretKey", "container", true, 20));
        replicas.add(new TransientKvs("transient", "C-accessKey", "C-secretKey", "container", true, 20));
        Metadata md = new Metadata(ts, hash, 0, replicas);

        mds.tsWrite(key, md, MdsManager.NONODE);

        md = mds.tsRead(key, null);
        assertEquals(ts, md.getTs());
        assertArrayEquals(hash, md.getHash());
        assertArrayEquals(replicas.toArray(), md.getReplicasLst().toArray());

        mds.delete(key);
        assertNull(mds.tsRead(key, null));
    }

    @Test
    public void testOverwrite() throws HybrisException {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "A-accessKey", "A-secretKey", "container", true, 20));
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        Stat stat = new Stat();
        Metadata retrieved;
        String cid1 = "ZZZ";
        String cid2 = "AAA";

        mds.tsWrite(key, new Metadata(new Timestamp(0, cid1), hash, 0, replicas), -1);  // znode does not exist, create hver. 0, zkver. 0
        mds.tsWrite(key, new Metadata(new Timestamp(0, cid2), hash, 0, replicas), -1);  // NODEEXISTS retries because AAA > ZZZ, write hver. 0, zkver. 1

        retrieved = mds.tsRead(key, stat);
        assertEquals(0, retrieved.getTs().getNum());
        assertEquals(cid2, retrieved.getTs().getCid());
        assertEquals(1, stat.getVersion());

        mds.tsWrite(key, new Metadata(new Timestamp(1, cid1), hash, 1, replicas), 1);   // write hver. 1, zkver. 2

        mds.tsWrite(key, new Metadata(new Timestamp(2, cid1), hash, 2, replicas), 2);   // write hver. 2, zkver. 3
        try {
            mds.tsWrite(key, new Metadata(new Timestamp(2, cid1), hash, 3, replicas), 2);   // BADVERSION, fails because cids are equals
            fail();
        } catch(HybrisException e) {  }
        mds.tsWrite(key, new Metadata(new Timestamp(2, cid2), hash, 4, replicas), 2);       // BADVERSION, retries because AAA > ZZZ, write hver. 2, zkver. 4

        retrieved = mds.tsRead(key, stat);
        assertEquals(2, retrieved.getTs().getNum());
        assertEquals(cid2, retrieved.getTs().getCid());
        assertEquals(4, stat.getVersion());

        try{
            mds.tsWrite(key, new Metadata(new Timestamp(0, cid1), hash, 5, replicas), 0);  // BADVERSION, fails because hver is smaller
            fail();
        } catch(HybrisException e) {  }

        mds.tsWrite(key, new Metadata(new Timestamp(3, cid1), hash, 6, replicas), 1);  // BADVERSION, retries because 3 > 2, write hver. 3, zkver. 5

        retrieved = mds.tsRead(key, stat);
        assertEquals(3, retrieved.getTs().getNum());
        assertEquals(cid1, retrieved.getTs().getCid());
        assertEquals(5, stat.getVersion());
    }

    @Test
    public void testDeleteNotExistingKey() {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        try {
            mds.delete(key);
        } catch (HybrisException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testReadNotExistingKey() {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        Metadata value = null;

        try {
            Stat stat = new Stat();
            stat.setVersion(MdsManager.NONODE);
            value = mds.tsRead(key, stat);
            assertNull(value);
            assertEquals(MdsManager.NONODE, stat.getVersion()); // in case of not existent znode, stat will remain unmodified
        } catch (HybrisException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testTimestampOrdering() {
        // Timestamps differing for the clientIds
        Timestamp t1 = new Timestamp(2, "AAAAA");
        Timestamp t2 = new Timestamp(2, "ZZZZZ");
        assertTrue(t1.isGreater(t2));
        assertFalse(t2.isGreater(t1));

        // Timestamps differing for the num
        t1.setNum(3);
        t2.setNum(4);
        assertTrue(t2.isGreater(t1));
        assertFalse(t1.isGreater(t2));

        // Equal timestamps
        Timestamp t3 = new Timestamp(2, "XXXXXX");
        Timestamp t4 = new Timestamp(2, "XXXXXX");
        assertFalse(t3.isGreater(t4));
        assertFalse(t4.isGreater(t3));
        assertTrue(t3.equals(t4));
    }

    @Test
    public void testSerialization() throws HybrisException {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        Timestamp ts = new Timestamp(new BigInteger(10, this.random).intValue(), Utils.getClientId());
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "A-accessKey", "A-secretKey", "container", true, 20));
        replicas.add(new TransientKvs("transient", "B-accessKey", "B-secretKey", "container", true, 20));
        replicas.add(new TransientKvs("transient", "C-accessKey", "C-secretKey", "container", true, 20));

        Metadata md = new Metadata(ts, hash, 7, replicas);
        mds.tsWrite(key, md, MdsManager.NONODE);

        md = mds.tsRead(key, null);
        for(Kvs provider : md.getReplicasLst()) {
            assertNotNull(provider.getId());
            assertEquals(replicas.get( md.getReplicasLst().indexOf(provider) ), provider);

            assertFalse(provider.isEnabled());
            assertEquals(0, provider.getReadLatency());
            assertEquals(0, provider.getWriteLatency());
            assertEquals(0, provider.getCost());
        }

        mds.delete(key);
        assertNull(mds.tsRead(key, null));
    }

    @Test
    public void testList() throws HybrisException {

        String key1 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key2 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key3 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key4 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key5 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        List<String> keys = new LinkedList<String>();
        keys.add(key1); keys.add(key2); keys.add(key3); keys.add(key4); keys.add(key5);
        Timestamp ts = new Timestamp(new BigInteger(10, this.random).intValue(), Utils.getClientId());
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "A-accessKey", "A-secretKey", "container", true, 20));
        replicas.add(new TransientKvs("transient", "B-accessKey", "B-secretKey", "container", true, 20));

        Metadata md = new Metadata(ts, hash, 8, replicas);

        mds.tsWrite(key1, md, MdsManager.NONODE);
        mds.tsWrite(key2, md, MdsManager.NONODE);
        mds.tsWrite(key3, md, MdsManager.NONODE);
        mds.tsWrite(key4, md, MdsManager.NONODE);
        mds.tsWrite(key5, md, MdsManager.NONODE);

        List<String> listedKeys = mds.list();
        assertEquals(5, listedKeys.size());
        for (String k : keys)
            assertTrue(listedKeys.contains(k));

        ts.inc(Utils.getClientId());
        byte[] hash1 = new byte[20];
        this.random.nextBytes(hash1);
        md = new Metadata(ts, hash1, 9, replicas);
        mds.tsWrite(key4, md, MdsManager.NONODE);  // overwrites a key
        listedKeys = mds.list();
        assertEquals(5, listedKeys.size());
        Stat stat = new Stat();
        Metadata newMd = mds.tsRead(key4, stat);
        assertFalse(Arrays.equals(newMd.getHash(), hash));
        assertEquals(1, stat.getVersion());

        mds.delete(key3);               // remove a key
        listedKeys = mds.list();
        assertEquals(4, listedKeys.size());
        for (String k : keys)
            if (!k.equals(key3))
                assertTrue(listedKeys.contains(k));
            else
                assertFalse(listedKeys.contains(k));

        for (String k : keys)           // remove all keys
            mds.delete(k);
        listedKeys = mds.list();
        assertEquals(0, listedKeys.size());

        ts.inc(Utils.getClientId());    // add a key previously removed
        md = new Metadata(ts, hash, 10, replicas);
        mds.tsWrite(key2, md, -1);
        listedKeys = mds.list();
        assertEquals(1, listedKeys.size());
        assertEquals(key2, listedKeys.get(0));
    }

    @Test
    public void testGetAll() throws HybrisException {

        String key1 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key2 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key3 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key4 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key5 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        List<String> keys = new LinkedList<String>();
        keys.add(key1); keys.add(key2); keys.add(key3); keys.add(key4); keys.add(key5);
        Timestamp ts = new Timestamp(new BigInteger(10, this.random).intValue(), Utils.getClientId());
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "A-accessKey", "A-secretKey", "container", true, 10));
        replicas.add(new TransientKvs("transient", "B-accessKey", "B-secretKey", "container", true, 20));

        Metadata md = new Metadata(ts, hash, 11, replicas);

        mds.tsWrite(key1, md, MdsManager.NONODE);
        mds.tsWrite(key2, md, MdsManager.NONODE);
        mds.tsWrite(key3, md, MdsManager.NONODE);
        mds.tsWrite(key4, md, MdsManager.NONODE);
        mds.tsWrite(key5, md, MdsManager.NONODE);

        Map<String, Metadata> allMd = mds.getAll();
        assertEquals(5, allMd.size());
        for (String k : keys) {
            assertTrue(allMd.keySet().contains(k));
            assertFalse(allMd.get(k).isTombstone());
            assertTrue(allMd.get(k).equals(md));
        }

        Timestamp ts4 = new Timestamp(ts.getNum() +1, Utils.getClientId());
        byte[] hash1 = new byte[20];
        this.random.nextBytes(hash1);
        Metadata md4 = new Metadata(ts4, hash1, 12, replicas);
        mds.tsWrite(key4, md4, MdsManager.NONODE);  // overwrites a key
        allMd = mds.getAll();
        assertEquals(5, allMd.size());
        Stat stat = new Stat();
        Metadata newMd = mds.tsRead(key4, stat);
        assertFalse(Arrays.equals(newMd.getHash(), hash));
        assertEquals(1, stat.getVersion());

        mds.delete(key3);                       // remove a key
        allMd = mds.getAll();
        assertEquals(4, allMd.size());
        for (String k : keys)
            if (!k.equals(key3)) {
                assertTrue(allMd.keySet().contains(k));
                assertFalse(allMd.get(k).isTombstone());
                if (k.equals(key4))
                    assertEquals(md4, allMd.get(k));
                else
                    assertEquals(md, allMd.get(k));
            } else
                assertFalse(allMd.keySet().contains(k));

        for (String k : keys)                   // remove all keys
            mds.delete(k);
        allMd = mds.getAll();
        assertEquals(0, allMd.size());

        ts.inc(Utils.getClientId());            // add a key previously removed
        md = new Metadata(ts, hash, 13, replicas);
        mds.tsWrite(key2, md, -1);
        allMd = mds.getAll();
        assertEquals(1, allMd.size());
        assertEquals(md, allMd.get(key2));
    }
}