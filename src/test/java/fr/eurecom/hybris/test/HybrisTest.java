package fr.eurecom.hybris.test;

import java.math.BigInteger;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;


public class HybrisTest extends HybrisAbstractTest {

    private Hybris hybris;
    
    @Before
    public void setUp() throws Exception {
        hybris = new Hybris();
    }

    @After
    public void tearDown() throws Exception {  }

    @Test
    public void testBasicWriteAndRead() throws HybrisException {
        
        String key = TEST_KEY_PREFIX + (new BigInteger(50, random).toString(32));
        byte[] value = (new BigInteger(50, random).toString(32)).getBytes();
        
        hybris.write(key, value);
        byte[] output = hybris.read(key);
        assertTrue(Arrays.equals(value, output));
        
        hybris.gc(key);
        
        try {
            hybris.read(key);
            fail();     // should not reach this point 
        } catch (HybrisException he) {  }
    }
}