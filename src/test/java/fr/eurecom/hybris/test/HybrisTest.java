package fr.eurecom.hybris.test;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;


public class HybrisTest extends TestCase {

    private String BASIC_KEY = "mykey";
    private String BASIC_VALUE = "my_value";
    
    private Hybris hybris;
    
    @Before
    public void setUp() throws Exception {
        hybris = new Hybris();
    }

    @After
    public void tearDown() throws Exception {  }

    @Test
    public void testBasicWriteAndRead() throws HybrisException {
        hybris.write(BASIC_KEY, BASIC_VALUE.getBytes());
        String output = new String(hybris.read(BASIC_KEY));
        assertEquals(BASIC_VALUE, output);
    }
}