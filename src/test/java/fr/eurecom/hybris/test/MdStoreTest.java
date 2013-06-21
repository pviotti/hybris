package fr.eurecom.hybris.test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.eurecom.hybris.mdstore.MdStore;
import fr.eurecom.hybris.mdstore.TsDir;


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
    public void testBasicWriteAndRead() throws IOException {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        long ts = System.currentTimeMillis();
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
}
