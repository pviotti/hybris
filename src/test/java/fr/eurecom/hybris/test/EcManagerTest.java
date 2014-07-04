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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.eurecom.hybris.EcManager;
import fr.eurecom.hybris.HybrisException;

public class EcManagerTest extends HybrisAbstractTest {

    protected static EcManager ecManager;
    
    @BeforeClass
    public static void beforeClassSetup() throws Exception {
        ecManager = new EcManager();
    }

    @Before
    public void setUp() throws Exception {  }

    @After
    public void tearDown() throws Exception {  }

    @Test
    public void testBasicEncodeDecode() throws HybrisException {

        int k = 2;
        int m = 1;
        
        byte[] data = new byte[1024];
        this.random.nextBytes(data);
        
        byte[][] encoded = ecManager.encode(data, k, m);

        // no erasures
        int[] erasures = new int[k+m];
        erasures[0] = -1;
        
        byte[][] dataC = new byte[k][];
        byte[][] coding = new byte[m][];
        for (int i=0; i<k; i++)
            dataC[i] = encoded[i];
        for (int i=0; i<m; i++)
            coding[i] = encoded[k+i];        
        
        byte[] decoded = ecManager.decode(dataC, coding, erasures, k, m, data.length);
        
        assertArrayEquals(data, decoded);
    }
    
    @Test
    public void testErasuresOk() throws HybrisException {

        int k = 2;
        int m = 2;
        
        byte[] data = new byte[1024];
        this.random.nextBytes(data);
        
        byte[][] encoded = ecManager.encode(data, k, m);

        byte[][] dataC = new byte[k][];
        byte[][] coding = new byte[m][];
        for (int i=0; i<k; i++)
            dataC[i] = encoded[i];
        for (int i=0; i<m; i++)
            coding[i] = encoded[k+i];
        
        // erasures
        int[] erasures = new int[k+m];
        erasures[0] = 1;
        erasures[1] = 3;
        erasures[2] = -1;
        dataC[1] = new byte[dataC[0].length];
        coding[3-k] = new byte[coding[0].length];
        
        byte[] decoded = ecManager.decode(dataC, coding, erasures, k, m, data.length);
        
        assertArrayEquals(data, decoded);
    }
    
    @Test
    public void testErasuresKo() throws HybrisException {

        int k = 2;
        int m = 2;
        
        byte[] data = new byte[1024];
        this.random.nextBytes(data);
        
        byte[][] encoded = ecManager.encode(data, k, m);

        byte[][] dataC = new byte[k][];
        byte[][] coding = new byte[m][];
        for (int i=0; i<k; i++)
            dataC[i] = encoded[i];
        for (int i=0; i<m; i++)
            coding[i] = encoded[k+i];
        
        // erasures
        int[] erasures = new int[k+m];
        erasures[0] = 2;
        erasures[1] = 3;    // NB: order of erasure does not matter
        erasures[2] = 1;
        erasures[3] = -1;
        
        dataC[1] = new byte[dataC[0].length];
        coding[2-k] = new byte[coding[0].length];
        coding[3-k] = new byte[coding[0].length];
        
        byte[] decoded = null;
        try {
            decoded = ecManager.decode(dataC, coding, erasures, k, m, data.length);
            fail();
        } catch (HybrisException e) {
            assertNull(decoded);
        }
    }
    
    @Test
    public void testSmallFile() throws HybrisException {

        int k = 2;
        int m = 2;
        
        byte[] data = new byte[5];
        this.random.nextBytes(data);
        
        byte[][] encoded = ecManager.encode(data, k, m);
        //System.out.println("Small size (5) block size: " + encoded[0].length);
        
        // no erasures
        int[] erasures = new int[k+m];
        erasures[0] = -1;
        
        byte[][] dataC = new byte[k][];
        byte[][] coding = new byte[m][];
        for (int i=0; i<k; i++)
            dataC[i] = encoded[i];
        for (int i=0; i<m; i++)
            coding[i] = encoded[k+i];        
        
        byte[] decoded = ecManager.decode(dataC, coding, erasures, k, m, data.length);
        
        assertArrayEquals(data, decoded);
    }
    
    @Test
    public void testBigFile() throws HybrisException {

        int k = 2;
        int m = 2;
        
        byte[] data = new byte[1024 * 1024 * 10];   // 10 MB
        this.random.nextBytes(data);
        
        byte[][] encoded = ecManager.encode(data, k, m);
        //System.out.println("Big size (10485760) block size: " + encoded[0].length);
        
        // no erasures
        int[] erasures = new int[k+m];
        erasures[0] = -1;
        
        byte[][] dataC = new byte[k][];
        byte[][] coding = new byte[m][];
        for (int i=0; i<k; i++)
            dataC[i] = encoded[i];
        for (int i=0; i<m; i++)
            coding[i] = encoded[k+i];        
        
        byte[] decoded = ecManager.decode(dataC, coding, erasures, k, m, data.length);
        
        assertArrayEquals(data, decoded);
    }
    
    @Test
    public void testKM() throws HybrisException {

        int[] kk = {10, 20, 25, 30, 17};
        int[] mm = {5, 7, 15, 20, 25};
        
        byte[] data = new byte[2048];
        this.random.nextBytes(data);
        
        for (int i =0; i<5; i++) {
            int k = kk[i];
            for (int j =0; j<5; j++) {
                int m = mm[j];
                //System.out.println(k + " - " + m);
                
                byte[][] encoded = ecManager.encode(data, k, m);
        
                // no erasures
                int[] erasures = new int[k+m];
                erasures[0] = -1;
                
                byte[][] dataC = new byte[k][];
                byte[][] coding = new byte[m][];
                for (int y=0; y<k; y++)
                    dataC[y] = encoded[y];
                for (int y=0; y<m; y++)
                    coding[y] = encoded[k+y];        
                
                byte[] decoded = ecManager.decode(dataC, coding, erasures, k, m, data.length);
                
                assertArrayEquals(data, decoded);
            }
        }
    }
}
