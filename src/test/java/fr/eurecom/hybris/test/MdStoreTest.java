package fr.eurecom.hybris.test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.CloudProvider;
import fr.eurecom.hybris.mds.MdStore;
import fr.eurecom.hybris.mds.Metadata;
import fr.eurecom.hybris.mds.Metadata.Timestamp;


public class MdStoreTest extends HybrisAbstractTest {
    
    private MdStore mds;
    
    private String MDS_ROOT = "mdstest-root";
    private String MDS_ADDRESS = "localhost:2181";

    @Before
    public void setUp() throws Exception {  
        mds = new MdStore(MDS_ADDRESS, MDS_ROOT);
        // TODO clean up test container
    }

    @After
    public void tearDown() throws Exception {  }

    @Test
    public void testBasicWriteAndRead() throws HybrisException {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        Timestamp ts = new Timestamp(new BigInteger(10, random).intValue(), Utils.getClientId());
        byte[] hash = (new BigInteger(50, random).toString(10)).getBytes();
        List<CloudProvider> replicas = new ArrayList<CloudProvider>();
        replicas.add(new CloudProvider("A", "A-accessKey", "A-secretKey", true, 0));
        replicas.add(new CloudProvider("B", "B-accessKey", "B-secretKey", true, 0));
        replicas.add(new CloudProvider("C", "C-accessKey", "C-secretKey", true, 0));
        Metadata tsdir = new Metadata(ts, hash, replicas);
        
        mds.tsWrite(key, tsdir);
        
        tsdir = new Metadata(mds.tsRead(key));
        assertEquals(ts, tsdir.getTs());
        assertTrue(Arrays.equals(hash, tsdir.getHash()));
        assertTrue(Arrays.equals(replicas.toArray(), tsdir.getReplicasLst().toArray()));
        
        mds.delete(key);
        assertNull(mds.tsRead(key));
    }
    
    @Test
    public void testOverwrite() throws HybrisException {
        
        Config.getInstance();
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        List<CloudProvider> replicas = new ArrayList<CloudProvider>();
        replicas.add(new CloudProvider("A", "A-accessKey", "A-secretKey", true, 0));
        byte[] hash = (new BigInteger(50, random).toString(10)).getBytes();
        String cid = Utils.getClientId();
        
        // XXX this causes infinite recursion 
//        mds.tsWrite(key, new Metadata(new Timestamp(0, cid), hash, replicas)); // [-1: write anyway] znode does not exist, create version 0
//        mds.tsWrite(key, new Metadata(new Timestamp(2, cid), hash, replicas)); // check for 1, but version is 0: retry
        
        mds.tsWrite(key, new Metadata(new Timestamp(0, cid), hash, replicas)); // znode does not exist, create version 0
        mds.tsWrite(key, new Metadata(new Timestamp(1, cid), hash, replicas)); // check for 1, but version is 0: retry
        mds.tsWrite(key, new Metadata(new Timestamp(2, cid), hash, replicas)); // check for 1, but version is 0: retry
        
        Metadata retrieved = new Metadata(mds.tsRead(key));
        assertEquals(2, retrieved.getTs().getNum());
    }
    
    @Test
    public void testDeleteNotExistingKey() {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        try {
            mds.delete(key);
        } catch (HybrisException e) {
            e.printStackTrace();
            fail();
        }
    }
    
    @Test
    public void testReadNotExistingKey() {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        byte[] value = null;
        
        try {
            value = mds.tsRead(key);
            assertNull(value);
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
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        Timestamp ts = new Timestamp(new BigInteger(10, random).intValue(), Utils.getClientId());
        byte[] hash = (new BigInteger(50, random).toString(10)).getBytes();
        List<CloudProvider> replicas = new ArrayList<CloudProvider>();
        replicas.add(new CloudProvider("A", "A-accessKey", "A-secretKey", true, 10));
        replicas.add(new CloudProvider("B", "B-accessKey", "B-secretKey", true, 20));
        replicas.add(new CloudProvider("C", "C-accessKey", "C-secretKey", true, 30));
        
        Metadata tsdir = new Metadata(ts, hash, replicas); 
        mds.tsWrite(key, tsdir);
        
        tsdir = new Metadata(mds.tsRead(key));
        for(CloudProvider provider : tsdir.getReplicasLst()) {
            assertNotNull(provider.getId());
            assertEquals(replicas.get( tsdir.getReplicasLst().indexOf(provider) ), provider);
            
            assertFalse(provider.isAlreadyUsed());
            assertFalse(provider.isEnabled());
            assertEquals(0, provider.getReadLatency());
            assertEquals(0, provider.getWriteLatency());
            assertEquals(0, provider.getCost());
            assertNull(provider.getAccessKey());
            assertNull(provider.getSecretKey());
        }
        
        mds.delete(key);
        assertNull(mds.tsRead(key));
    }
    
    // TODO TEMP
    public static void main(String[] args) throws Exception {
        MdStoreTest t = new MdStoreTest();
        t.setUp();
        t.testOverwrite();
    }
}