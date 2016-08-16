/**
 * Copyright (C) 2013 EURECOM (www.eurecom.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.eurecom.hybris.test.mds;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.kvs.drivers.TransientKvs;
import fr.eurecom.hybris.mds.ZkRmds;
import fr.eurecom.hybris.mds.ConsulRmds;
import fr.eurecom.hybris.mds.Metadata;
import fr.eurecom.hybris.mds.Metadata.Timestamp;
import fr.eurecom.hybris.test.HybrisAbstractTest;


public class ConsulRmdsTest extends HybrisAbstractTest {

    private static ConsulRmds mds;
    private static String MDS_TEST_ROOT = "mdstest-root";

    @BeforeClass
    public static void beforeClassSetup() throws Exception {
        Config.getInstance();
        //zkTestingServer = new TestingServer();
        mds = new ConsulRmds("localhost", MDS_TEST_ROOT);
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

        String cid = Utils.generateClientId();
        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        Timestamp ts = new Timestamp(new BigInteger(10, this.random).intValue(), cid);
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "A-accessKey", "A-secretKey", "container", true, 20));
        replicas.add(new TransientKvs("transient", "B-accessKey", "B-secretKey", "container", true, 20));
        replicas.add(new TransientKvs("transient", "C-accessKey", "C-secretKey", "container", true, 20));
        Metadata md = new Metadata(ts, hash, 0, replicas, null);

        mds.tsWrite(key, md, ZkRmds.NONODE);

        Stat stat = new Stat();
        md = mds.tsRead(key, stat);
        assertEquals(ts, md.getTs());
        assertArrayEquals(hash, md.getHash());
        assertArrayEquals(replicas.toArray(), md.getReplicasLst().toArray());

        ts.inc(cid);
        Metadata tombstone = Metadata.getTombstone(ts);
        mds.delete(key, tombstone, stat.getVersion());
        Stat newStat = new Stat();
        md = mds.tsRead(key, newStat);
        assertNotNull(md);
        assertEquals(md.getTs(), tombstone.getTs());
        assertNull(md.getReplicasLst());
        assertNull(md.getHash());
        assertEquals(0, md.getSize());
        assertTrue(md.isTombstone());
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

        mds.tsWrite(key, new Metadata(new Timestamp(0, cid1), hash, 0, replicas, null), -1);  // znode does not exist, create hver. 0, zkver. 0
        mds.tsWrite(key, new Metadata(new Timestamp(0, cid2), hash, 0, replicas, null), -1);  // NODEEXISTS retries because AAA > ZZZ, write hver. 0, zkver. 1

        retrieved = mds.tsRead(key, stat);
        assertEquals(0, retrieved.getTs().getNum());
        assertEquals(cid2, retrieved.getTs().getCid());
        //assertEquals(1, stat.getVersion()); // XXX Consul md versioning is different from Zk's

        mds.tsWrite(key, new Metadata(new Timestamp(1, cid1), hash, 1, replicas, null), 1);   // write hver. 1, zkver. 2

        mds.tsWrite(key, new Metadata(new Timestamp(2, cid1), hash, 2, replicas, null), 2);   // write hver. 2, zkver. 3
        try {
            mds.tsWrite(key, new Metadata(new Timestamp(2, cid1), hash, 3, replicas, null), 2);   // BADVERSION, fails because cids are equals
        } catch(HybrisException e) {
            e.printStackTrace();
            fail(); // TODO modify to test the new version which does not throw an exception
        }
        mds.tsWrite(key, new Metadata(new Timestamp(2, cid2), hash, 4, replicas, null), 2);       // BADVERSION, retries because AAA > ZZZ, write hver. 2, zkver. 4

        retrieved = mds.tsRead(key, stat);
        assertEquals(2, retrieved.getTs().getNum());
        assertEquals(cid2, retrieved.getTs().getCid());
        //assertEquals(4, stat.getVersion()); // XXX Consul md versioning is different from Zk's

        try{
            mds.tsWrite(key, new Metadata(new Timestamp(0, cid1), hash, 5, replicas, null), 0);  // BADVERSION, fails because hver is smaller
        } catch(HybrisException e) {
            e.printStackTrace();
            fail(); // TODO see above
        }

        mds.tsWrite(key, new Metadata(new Timestamp(3, cid1), hash, 6, replicas, null), 1);  // BADVERSION, retries because 3 > 2, write hver. 3, zkver. 5

        retrieved = mds.tsRead(key, stat);
        assertEquals(3, retrieved.getTs().getNum());
        assertEquals(cid1, retrieved.getTs().getCid());
        //assertEquals(5, stat.getVersion()); // XXX Consul md versioning is different from Zk's
    }
    
    @Ignore
    @Test 
    public void testTransactionalWrite() {
        
        LinkedHashMap<String, Metadata> map = new LinkedHashMap<String, Metadata>();
        LinkedHashMap<String, Stat> statMap = new LinkedHashMap<String, Stat>();
        int numKeys = 5, i = 0;
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "b", "c", "d", true, 20));
        Metadata retrieved;
        for (i=0; i<numKeys; i++) {
            String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
            map.put(key, new Metadata(new Timestamp(this.random.nextInt(10), "qwe"), hash, 2, replicas, null));
            Stat st = new Stat();
            st.setVersion(-1);
            statMap.put(key, st);
        }
        
        // successful transactional write creating znodes
        try {
            mds.tsMultiWrite(map, statMap);
            for (Entry<String, Metadata> entry : map.entrySet()) {
                retrieved = mds.tsRead(entry.getKey(), new Stat());
                assertEquals(entry.getValue(), retrieved);
            }
        } catch (HybrisException e) {
            fail(e.getMessage());
        }            
       
        // fails for mismatching versions of all the keys
        try {
            mds.tsMultiWrite(map, statMap);
            fail();
        } catch (HybrisException e) {
            KeeperException ke = (KeeperException) e.getCause();
            assertEquals(KeeperException.Code.NODEEXISTS, ke.code());
        }
        
        // successful transactional write modifying all znodes
        for (Entry<String, Stat> entry : statMap.entrySet()) 
            entry.getValue().setVersion(entry.getValue().getVersion() + 1);
        try {
            for (Entry<String, Metadata> entry : map.entrySet())
                map.put(entry.getKey(),
                        new Metadata(new Timestamp(this.random.nextInt(10), "NEW"), hash, 10, replicas, null)); 
            mds.tsMultiWrite(map, statMap);
            for (Entry<String, Metadata> entry : map.entrySet()) {
                retrieved = mds.tsRead(entry.getKey(), new Stat());
                assertEquals(entry.getValue(), retrieved);
            }
        } catch (HybrisException e) {
            fail(e.getMessage());
        }
        
        // successful transactional write with mix of modify and create nodes
        for (Entry<String, Stat> entry : statMap.entrySet()) 
            entry.getValue().setVersion(entry.getValue().getVersion() + 1);
        int numNewKeys = 3;
        for (i=0; i<numNewKeys; i++) {
            String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
            map.put(key, new Metadata(new Timestamp(this.random.nextInt(10), "qwe"), hash, 2, replicas, null));
            Stat st = new Stat();
            st.setVersion(-1);
            statMap.put(key, st);
        }
        try {
            mds.tsMultiWrite(map, statMap);
            for (Entry<String, Metadata> entry : map.entrySet()) {
                retrieved = mds.tsRead(entry.getKey(), new Stat());
                assertEquals(entry.getValue(), retrieved);
            }
        } catch (HybrisException e) {
            fail(e.getMessage());
        }   
        
        // fails for mismatching version of one key
        for (Entry<String, Stat> entry : statMap.entrySet()){
            if (this.random.nextInt(10) > 5)
                entry.getValue().setVersion(entry.getValue().getVersion() + 1);
            else
                entry.getValue().setVersion(123);
        }
        try {
            mds.tsMultiWrite(map, statMap);
            fail();
        } catch (HybrisException e) {
            KeeperException ke = (KeeperException) e.getCause();
            assertEquals(KeeperException.Code.BADVERSION, ke.code());
        }
    }
    
    @Ignore
    @Test
    public void testTransactionalRead() {
        LinkedHashMap<String, Metadata> map = new LinkedHashMap<String, Metadata>();
        LinkedHashMap<String, Stat> statMap = new LinkedHashMap<String, Stat>();
        int numKeys = 10, i = 0;
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "b", "c", "d", true, 20));
        for (i=0; i<numKeys; i++) {
            String k = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
            map.put(k, new Metadata(new Timestamp(this.random.nextInt(10), "qwe"), hash, 2, replicas, null));
            Stat st = new Stat();
            st.setVersion(-1);
            statMap.put(k, st);
        }
        
        try {
            mds.tsMultiWrite(map, statMap);
        } catch (HybrisException e) {
            fail(e.getMessage());
        }
        
        try {
            LinkedHashMap<String, Metadata> res = mds.tsMultiRead(statMap);
            for (Entry<String, Metadata> entry : res.entrySet())
                assertEquals(map.get(entry.getKey()), entry.getValue());               
            
        } catch (HybrisException e) {
            fail(e.getMessage());
        }   
    }

    @Test
    public void testDeleteNotExistingKey() {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        try {
            Timestamp ts1 = new Timestamp(1, "BBBB");
            Timestamp ts2 = new Timestamp(1, "AAAA");
            Timestamp ts3 = new Timestamp(1, "CCCC");

            mds.delete(key, Metadata.getTombstone(ts1), -1);     // writes the tombstone no matter which znode version
            mds.delete(key, Metadata.getTombstone(ts2), 5);      // finds a smaller znode version, retries (because ts is greater) and eventually succeeds
            mds.delete(key, Metadata.getTombstone(ts3), 5);      // finds a smaller znode version, silently fails

            Stat stat = new Stat();
            Metadata md = mds.tsRead(key, stat);
            assertNotNull(md);
            assertEquals(md.getTs(), ts2);
            assertNull(md.getReplicasLst());
            assertNull(md.getHash());
            assertEquals(0, md.getSize());
            assertTrue(md.isTombstone());
        } catch (HybrisException e) {
            e.printStackTrace();
            fail();
        }

        key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        try {
            mds.delete(key, Metadata.getTombstone(new Timestamp(1, "QWERTY")), 7);     // writes the tombstone anyway
            fail();
        } catch (HybrisException e) {
        	assert(e.getMessage().contains("not found"));
//            KeeperException ke = (KeeperException) e.getCause();
//            assertEquals(KeeperException.Code.NONODE, ke.code());
        }
    }

    @Test
    public void testTombstoneTimestamp() {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "A-accessKey", "A-secretKey", "container", true, 20));
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        String cid1 = "ZZZ";

        try {
            int n = 0;
            mds.tsWrite(key, new Metadata(new Timestamp(n, cid1), hash, 0, replicas, null), -1);      // znode does not exist, create hver. 0, zkver. 0

            n++;
            mds.delete(key, Metadata.getTombstone(new Timestamp(n, cid1)), 0);

            Stat stat = new Stat();
            Metadata readMd = mds.tsRead(key, stat);
            assertTrue(readMd.isTombstone());
            assertEquals(n, readMd.getTs().getNum());
            assertEquals(cid1, readMd.getTs().getCid());
            //assertEquals(1, stat.getVersion()); // XXX Consul md versioning is different from Zk's
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
            stat.setVersion(ZkRmds.NONODE);
            value = mds.tsRead(key, stat);
            assertNull(value);
            assertEquals(ZkRmds.NONODE, stat.getVersion()); // in case of not existent znode, stat will remain unmodified
        } catch (HybrisException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testSerialization() throws HybrisException {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        Timestamp ts = new Timestamp(new BigInteger(10, this.random).intValue(), Utils.generateClientId());
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "A-accessKey", "A-secretKey", "container", true, 20));
        replicas.add(new TransientKvs("transient", "B-accessKey", "B-secretKey", "container", true, 20));
        replicas.add(new TransientKvs("transient", "C-accessKey", "C-secretKey", "container", true, 20));

        Metadata md = new Metadata(ts, hash, 7, replicas, null);
        mds.tsWrite(key, md, ZkRmds.NONODE);

        Stat stat = new Stat();
        md = mds.tsRead(key, stat);
        for(Kvs provider : md.getReplicasLst()) {
            assertNotNull(provider.getId());
            assertEquals(replicas.get( md.getReplicasLst().indexOf(provider) ), provider);

            assertFalse(provider.isEnabled());
            assertEquals(0, provider.getReadLatency());
            assertEquals(0, provider.getWriteLatency());
            assertEquals(0, provider.getCost());
        }

        mds.delete(key, Metadata.getTombstone(ts), stat.getVersion());
        assertTrue(mds.tsRead(key, null).isTombstone());
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
        Timestamp ts = new Timestamp(new BigInteger(10, this.random).intValue(), Utils.generateClientId());
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "A-accessKey", "A-secretKey", "container", true, 20));
        replicas.add(new TransientKvs("transient", "B-accessKey", "B-secretKey", "container", true, 20));

        Metadata md = new Metadata(ts, hash, 8, replicas, null);

        mds.tsWrite(key1, md, ZkRmds.NONODE);
        mds.tsWrite(key2, md, ZkRmds.NONODE);
        mds.tsWrite(key3, md, ZkRmds.NONODE);
        mds.tsWrite(key4, md, ZkRmds.NONODE);
        mds.tsWrite(key5, md, ZkRmds.NONODE);

        List<String> listedKeys = mds.list();
        assertEquals(5, listedKeys.size());
        for (String k : keys)
            assertTrue(listedKeys.contains(k));

        ts.inc(Utils.generateClientId());
        byte[] hash1 = new byte[20];
        this.random.nextBytes(hash1);
        md = new Metadata(ts, hash1, 9, replicas, null);
        mds.tsWrite(key4, md, ZkRmds.NONODE);  // overwrites a key
        listedKeys = mds.list();
        assertEquals(5, listedKeys.size());
        Stat stat = new Stat();
        Metadata newMd = mds.tsRead(key4, stat);
        assertFalse(Arrays.equals(newMd.getHash(), hash));
        //assertEquals(1, stat.getVersion()); // XXX Consul md versioning is different from Zk's

        ts.inc("clientXYZ");
        mds.delete(key3, Metadata.getTombstone(ts), stat.getVersion());      // remove a key
        assertTrue(mds.tsRead(key3, null).isTombstone());
        listedKeys = mds.list();
        assertEquals(4, listedKeys.size());
        for (String k : keys)
            if (!k.equals(key3))
                assertTrue(listedKeys.contains(k));
            else
                assertFalse(listedKeys.contains(k));

        Timestamp tsd = new Timestamp(999, "AAA");
        for (String k : keys)                               // remove all keys
            mds.delete(k, Metadata.getTombstone(tsd), -1);
        listedKeys = mds.list();
        assertEquals(0, listedKeys.size());

        Timestamp smallerTs = new Timestamp(1, "ZZZ");      // add a key previously removed with a smaller ts: not written
        ts.inc(Utils.generateClientId());
        md = new Metadata(smallerTs, hash, 10, replicas, null);
        mds.tsWrite(key2, md, ZkRmds.NONODE);
        listedKeys = mds.list();
        assertEquals(0, listedKeys.size());

        tsd.inc("AAAA");                                    // add a key previously removed with a greater ts: written
        md = new Metadata(tsd, hash, 10, replicas, null);
        mds.tsWrite(key2, md, ZkRmds.NONODE);
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
        Timestamp ts = new Timestamp(new BigInteger(10, this.random).intValue(), Utils.generateClientId());
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "A-accessKey", "A-secretKey", "container", true, 10));
        replicas.add(new TransientKvs("transient", "B-accessKey", "B-secretKey", "container", true, 20));

        Metadata md = new Metadata(ts, hash, 11, replicas, null);

        mds.tsWrite(key1, md, ZkRmds.NONODE);
        mds.tsWrite(key2, md, ZkRmds.NONODE);
        mds.tsWrite(key3, md, ZkRmds.NONODE);
        mds.tsWrite(key4, md, ZkRmds.NONODE);
        mds.tsWrite(key5, md, ZkRmds.NONODE);

        Map<String, Metadata> allMd = mds.getAll();
        assertEquals(5, allMd.size());
        for (String k : keys) {
            assertTrue(allMd.keySet().contains(k));
            assertFalse(allMd.get(k).isTombstone());
            assertTrue(allMd.get(k).equals(md));
        }

        Timestamp ts4 = new Timestamp(ts.getNum() + 1, Utils.generateClientId());
        byte[] hash1 = new byte[20];
        this.random.nextBytes(hash1);
        Metadata md4 = new Metadata(ts4, hash1, 12, replicas, null);
        mds.tsWrite(key4, md4, ZkRmds.NONODE);  // overwrites a key
        allMd = mds.getAll();
        assertEquals(5, allMd.size());
        Stat stat = new Stat();
        Metadata newMd = mds.tsRead(key4, stat);
        assertFalse(Arrays.equals(newMd.getHash(), hash));
        //assertEquals(1, stat.getVersion());

        Timestamp newts = new Timestamp(ts.getNum() + 1, "clientXYZ");
        mds.delete(key3, Metadata.getTombstone(newts), stat.getVersion());      // remove a key
        assertTrue(mds.tsRead(key3, null).isTombstone());
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

        Timestamp tsd = new Timestamp(999, "AAA");
        for (String k : keys)                   // remove all keys
            mds.delete(k, Metadata.getTombstone(tsd), -1);
        allMd = mds.getAll();
        assertEquals(0, allMd.size());

        tsd.inc("AAAA");                                    // add a key previously removed with a greater ts: written
        md = new Metadata(tsd, hash, 10, replicas, null);
        mds.tsWrite(key2, md, -1);
        allMd = mds.getAll();
        assertEquals(1, allMd.size());
        assertEquals(md, allMd.get(key2));
    }
}