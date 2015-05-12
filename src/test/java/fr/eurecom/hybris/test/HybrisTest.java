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
package fr.eurecom.hybris.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.util.LinkedHashMap;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import fr.eurecom.hybris.GcManager;
import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;


public class HybrisTest extends HybrisAbstractTest {

    protected static Hybris hybris;

    @BeforeClass
    public static void beforeClassSetup() throws Exception {
        zkTestingServer = new TestingServer(2181);
        hybris = new Hybris("hybris.properties");
    }

    @Before
    public void setUp() throws Exception {  }

    @After
    public void tearDown() throws Exception {  }

    @Test
    public void testBasicWriteAndRead() throws HybrisException {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value = new byte[50];
        this.random.nextBytes(value);
        
        hybris.testLatencyAndSortClouds(100);
        hybris.put(key, value);
        byte[] output = hybris.get(key);
        assertArrayEquals(value, output);

        hybris.delete(key);
        assertNull(hybris.get(key));
    }
    
    @Test
    public void testMultipleWrite() {
        
        LinkedHashMap<String, byte[]> map = new LinkedHashMap<String, byte[]>();
        int num = 10;
        byte[] value = new byte[50];
        for (int i=0; i<num; i++) {
            this.random.nextBytes(value);
            map.put(this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32), 
                    value);
        }
        
        hybris.setErasureCoding(false); // XXX
        
        try {
            hybris.put(map);
        } catch (HybrisException e) {
            e.printStackTrace();
            fail();
        }
        
        for (String key : map.keySet()) {
            byte[] val = null;
            try {
                val = hybris.get(key);
            } catch (HybrisException e) {
                e.printStackTrace();
                fail();
            }
            assertEquals(map.get(key), val);
        }        
    }
    
    @Test
    public void testEcFaultyKvs() throws HybrisException {

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value = new byte[50];
        this.random.nextBytes(value);
        
        Hybris hec = new Hybris(zkTestingServer.getConnectString(), "hybris", "accounts-test.properties",
                "hybris-test-container", false, null, 1, 600, 600, false, false, false, "", 0, "", true, 2);
        
        //hybris.testLatencyAndSortClouds(100);
        hec.put(key, value);
        byte[] output = hec.get(key);
        assertArrayEquals(value, output);

        hec.delete(key);
        assertNull(hec.get(key));
    }
    
    @Test
    public void testReplicationErasureCoding() throws HybrisException {

        String key1 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        String key2 = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value = new byte[1048576];
        this.random.nextBytes(value);
        
        Hybris hr = new Hybris(zkTestingServer.getConnectString(), "hybris", "accounts.properties", 
                "hybris-test-container", true, null, 1, 600, 600, false, false, false, "", 0, "", false, 0);
        Hybris hec = new Hybris(zkTestingServer.getConnectString(), "hybris", "accounts.properties",
                "hybris-test-container", true, null, 1, 600, 600, false, false, false, "", 0, "", true, 2);
        
        long start = 0, end = 0;
        
        start = System.currentTimeMillis();
        hr.put(key1, value);        
        end = System.currentTimeMillis();
        System.out.println("Replication put: " + (end - start) + " ms");
        
        start = System.currentTimeMillis();
        hec.put(key2, value);        
        end = System.currentTimeMillis();
        System.out.println("EC put: " + (end - start) + " ms");
        
        start = System.currentTimeMillis();
        byte[] output1 = hr.get(key1);
        end = System.currentTimeMillis();
        System.out.println("Replication get: " + (end - start) + " ms");
        
        
        start = System.currentTimeMillis();
        byte[] output2 = hec.get(key2);
        end = System.currentTimeMillis();
        System.out.println("EC get: " + (end - start) + " ms");
        
        assertArrayEquals(value, output1);
        assertArrayEquals(value, output2);

        hr.delete(key1);
        assertNull(hr.get(key1));
        hec.delete(key2);
        assertNull(hec.get(key2));
        
        new GcManager(hr)._emptyContainers();
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
        hybris.put(key1, payload);
        end = System.currentTimeMillis();
        System.out.println("Parallel write: " + (end - start) + " ms");
        hybris.delete(key1);
    }
}