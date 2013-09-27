package fr.eurecom.hybris.test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;


public class HybrisTest extends HybrisAbstractTest {

    protected static Hybris hybris;

    @BeforeClass
    public static void beforeClassSetup() throws HybrisException {
        hybris = new Hybris("hybris.properties");
    }

    @Before
    public void setUp() throws Exception {  }

    @After
    public void tearDown() throws Exception {  }

    @Test
    public void testBasicWriteAndRead() throws HybrisException {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value = new BigInteger(50, this.random).toString(32).getBytes();

        hybris.write(key, value);
        byte[] output = hybris.read(key);
        assertTrue(Arrays.equals(value, output));

        hybris.delete(key);
        assertNull(hybris.read(key));
    }

    //    @Ignore
    //    @Test
    //    public void testParallelWrite() throws HybrisException {
    //
    //        String key1 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
    //        String key2 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
    //        byte[] value1 = this.generatePayload(10493000, (byte) 'x'); // 2 MB = 2097152, 5MB = 5.243e+6
    //        byte[] value2 = this.generatePayload(10493000, (byte) 'y'); // 2 MB = 2097152, 5MB = 5.243e+6
    //
    //        long start = 0, end = 0;
    //
    //        start = System.currentTimeMillis();
    //        hybris.write(key1, value1);
    //        end = System.currentTimeMillis();
    //        System.out.println("Parallel write: " + (end - start) + " ms");
    //
    //        byte[] output = hybris.read(key1);
    //        assertTrue(Arrays.equals(value1, output));
    //        hybris.delete(key1);
    //        assertNull(hybris.read(key1));
    //
    //        start = System.currentTimeMillis();
    //        hybris.writeSerial(key2, value2);
    //        end = System.currentTimeMillis();
    //        System.out.println("Serial write: " + (end - start) + " ms");
    //
    //        output = hybris.read(key2);
    //        assertTrue(Arrays.equals(value2, output));
    //        hybris.delete(key2);
    //        assertNull(hybris.read(key2));
    //    }

    @Ignore
    @Test
    public void testLargeFiles() throws HybrisException {
        byte[] payload = this.generatePayload(50000000, (byte) 'x');
        String key1 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);

        long start = 0, end = 0;
        start = System.currentTimeMillis();
        hybris.write(key1, payload);
        end = System.currentTimeMillis();
        System.out.println("Parallel write: " + (end - start) + " ms");
        hybris.delete(key1);
    }
}