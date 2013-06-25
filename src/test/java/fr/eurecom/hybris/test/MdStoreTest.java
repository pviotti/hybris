package fr.eurecom.hybris.test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.mdstore.MdStore;
import fr.eurecom.hybris.mdstore.TsDir;
import fr.eurecom.hybris.mdstore.TsDir.Timestamp;


public class MdStoreTest extends HybrisAbstractTest {
    
    private MdStore mds;
    
    private String MDS_ROOT = "mdstest-root";
    private String MDS_ADDRESS = "localhost:2181";

    @Before
    public void setUp() throws Exception {  
        mds = new MdStore(MDS_ADDRESS, MDS_ROOT);
    }

    @After
    public void tearDown() throws Exception {  }

    @Test
    public void testBasicWriteAndRead() throws HybrisException {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        Timestamp ts = new Timestamp(new BigInteger(10, random).intValue(), Utils.getClientId());
        byte[] hash = (new BigInteger(50, random).toString(10)).getBytes();
        List<String> replicas = Arrays.asList("a","b","c");
        
        TsDir tsdir = new TsDir(ts, hash, replicas); 
        mds.tsWrite(key, tsdir);
        
        tsdir = new TsDir(mds.tsRead(key));
        
        assertEquals(ts, tsdir.getTs());
        assertTrue(Arrays.equals(hash, tsdir.getHash()));
        assertTrue(Arrays.equals(replicas.toArray(), tsdir.getReplicasLst().toArray()));
        
        mds.delete(key);
        
        assertNull(mds.tsRead(key));
    }
    
    @Test
    public void testTimestampOrdering() {
        // Timestamps differing for the clientIds
        Timestamp t1 = new Timestamp(2, "AAAAA");
        Timestamp t2 = new Timestamp(2, "ZZZZZ");
        assertTrue(t1.isGreater(t2));
        assertTrue(!t2.isGreater(t1));
        
        // Timestamps differing for the num
        t1.setNum(3);
        t2.setNum(4);
        assertTrue(t2.isGreater(t1));
        assertTrue(!t1.isGreater(t2));
        
        // Equal timestamps
        Timestamp t3 = new Timestamp(2, "XXXXXX");
        Timestamp t4 = new Timestamp(2, "XXXXXX");
        assertTrue(!t3.isGreater(t4));
        assertTrue(!t4.isGreater(t3));
        assertTrue(t3.equals(t4));
    }
}
