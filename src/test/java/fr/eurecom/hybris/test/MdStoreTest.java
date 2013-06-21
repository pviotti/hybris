package fr.eurecom.hybris.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.eurecom.hybris.mdstore.MdStore;
import fr.eurecom.hybris.mdstore.TsDir;
import junit.framework.TestCase;


public class MdStoreTest extends TestCase {

    private String BASIC_KEY = "testkey";
    
    private String MDS_ROOT = "mdstest-root";
    private String MDS_ADDRESS = "localhost:2181";
    
    private MdStore mds;
    
    @Before
    public void setUp() throws Exception {  
        mds = new MdStore(MDS_ADDRESS, MDS_ROOT);
    }

    @After
    public void tearDown() throws Exception {  }

    @Test
    public void testBasicWriteAndRead() throws IOException {
        
        long ts = System.currentTimeMillis();
        byte[] hash = "mockedhashvalue".getBytes();
        List<String> replicas = Arrays.asList("a","b","c");
        
        TsDir tsdir = new TsDir(ts, hash, replicas); 
        mds.tsWrite(BASIC_KEY, tsdir);
        
        tsdir = new TsDir(mds.tsRead(BASIC_KEY));
        
        assertEquals(ts, tsdir.getTs());
        assertTrue(Arrays.equals(hash, tsdir.getHash()));
        assertTrue(Arrays.equals(replicas.toArray(), tsdir.getReplicasLst().toArray()));
    }
}
