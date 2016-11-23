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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.kvs.drivers.TransientKvs;
import fr.eurecom.hybris.mds.Metadata;
import fr.eurecom.hybris.mds.Metadata.Timestamp;
import fr.eurecom.hybris.test.HybrisAbstractTest;

public class MetadataTest extends HybrisAbstractTest {

    @Test
    public void testSerialization() {

        int n = this.random.nextInt(100);
        int size = this.random.nextInt(5000);
        Timestamp ts = new Timestamp(n, Utils.generateClientId());
        byte[] hash = new byte[Utils.HASH_LENGTH];
        byte[] cryptoKey = new byte[Utils.CRYPTO_KEY_LENGTH];
        this.random.nextBytes(hash);
        this.random.nextBytes(cryptoKey);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "container", true, 20));
        replicas.add(new TransientKvs("transient", "container", true, 20));
        replicas.add(new TransientKvs("transient", "container", true, 20));
        Metadata md = new Metadata(ts, hash, size, replicas, cryptoKey);

        byte[] serialized = md.serialize();
        System.out.println("Metadata size (B): " + serialized.length);
        assertNotNull(serialized);

        Metadata mddes = new Metadata(serialized);
        assertEquals(md, mddes);

        for(Kvs provider : mddes.getReplicasLst()) {
            assertNotNull(provider.getId());
            assertEquals(replicas.get( md.getReplicasLst().indexOf(provider) ), provider);

            assertFalse(provider.isEnabled());
            assertEquals(0, provider.getReadLatency());
            assertEquals(0, provider.getWriteLatency());
            assertEquals(0, provider.getCost());
        }

        assertEquals(3, mddes.getReplicasLst().size());
        assertArrayEquals(replicas.toArray(), mddes.getReplicasLst().toArray());
        assertArrayEquals(hash, mddes.getHash());
        assertEquals(size, mddes.getSize());
        assertEquals(ts, mddes.getTs());
    }
    
    @Test
    public void testEcSerialization() {

        int n = this.random.nextInt(100);
        int size = this.random.nextInt(5000);
        Timestamp ts = new Timestamp(n, Utils.generateClientId());
        byte[][] chunksHashes = new byte[3][Utils.HASH_LENGTH];
        for (int i=0; i<3; i++)
            this.random.nextBytes(chunksHashes[i]);    
        byte[] cryptoKey = new byte[Utils.CRYPTO_KEY_LENGTH];
        this.random.nextBytes(cryptoKey);
        //byte[] cryptoKey = null;
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "container", true, 20));
        replicas.add(new TransientKvs("transient", "container", true, 20));
        replicas.add(new TransientKvs("transient", "container", true, 20));
        Metadata md = new Metadata(ts, chunksHashes, replicas, size, cryptoKey);

        byte[] serialized = md.serialize();
        System.out.println("Ec metadata size (B): " + serialized.length);
        assertNotNull(serialized);

        Metadata mddes = new Metadata(serialized);
        assertEquals(md, mddes);

        for(Kvs provider : mddes.getReplicasLst()) {
            assertNotNull(provider.getId());
            assertEquals(replicas.get( md.getReplicasLst().indexOf(provider) ), provider);

            assertFalse(provider.isEnabled());
            assertEquals(0, provider.getReadLatency());
            assertEquals(0, provider.getWriteLatency());
            assertEquals(0, provider.getCost());
        }

        assertEquals(3, mddes.getReplicasLst().size());
        assertArrayEquals(replicas.toArray(), mddes.getReplicasLst().toArray());
        for (int i=0; i<3; i++)
            assertArrayEquals(chunksHashes[i], mddes.getChunksHashes()[i]);
        assertEquals(size, mddes.getSize());
        assertEquals(ts, mddes.getTs());
    }

    @Test
    public void testSerializationCornerCases() {

        int n = this.random.nextInt(100);
        int size = this.random.nextInt(5000);
        Timestamp ts = new Timestamp(n, Utils.generateClientId());
        byte[] hash = new byte[Utils.HASH_LENGTH];
        this.random.nextBytes(hash);
        byte[] cryptoKey = new byte[Utils.CRYPTO_KEY_LENGTH];
        this.random.nextBytes(cryptoKey);

        // replica list == null
        List<Kvs> replicas = null;
        Metadata md = new Metadata(ts, hash, size, replicas, cryptoKey);
        byte[] serialized = md.serialize();
        assertNotNull(serialized);
        Metadata mddes = new Metadata(serialized);
        assertEquals(md, mddes);

        // replica list is empty
        replicas = new ArrayList<Kvs>();
        md = new Metadata(ts, hash, size, replicas, cryptoKey);
        serialized = md.serialize();
        assertNotNull(serialized);
        mddes = new Metadata(serialized);
        assertEquals(md, mddes);


        // replica list contains a not supported kvs id
        replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "container", true, 20));
        replicas.add(new TransientKvs("tranNOTEXISTENTsient", "container", true, 20));
        replicas.add(new TransientKvs("transient", "container", true, 20));
        md = new Metadata(ts, hash, size, replicas, cryptoKey);

        try {
            serialized = md.serialize();
        } catch (Exception e) {
            fail();
        }
        assertNotNull(serialized);

        mddes = new Metadata(serialized);
        assertNotEquals(md, mddes);
        assertArrayEquals(hash, mddes.getHash());
        assertEquals(size, mddes.getSize());
        assertEquals(ts, mddes.getTs());
        assertEquals(2, mddes.getReplicasLst().size());
    }

    @Test
    public void testTombstone() {
        int n = this.random.nextInt(100);
        Timestamp ts = new Timestamp(n, Utils.generateClientId());
        Metadata md = Metadata.getTombstone(ts);
        assertTrue(md.isTombstone());
        assertNull(md.getReplicasLst());
        assertNull(md.getHash());
        assertNull(md.getCryptoKey());
        assertNotNull(md.getTs());
        assertEquals(0, md.getSize());
        assertEquals(ts, md.getTs());
        
        byte[] serialized = md.serialize();
        System.out.println("Tombstone size (B): " + serialized.length);
        assertNotNull(serialized);

        Metadata mddes = new Metadata(serialized);
        assertEquals(md, mddes);
    }

    @Test
    public void testTimestamp() {
        int n = this.random.nextInt(100);
        String client1 = Utils.generateClientId();
        String client2 = Utils.generateClientId();

        Timestamp ts = new Timestamp(n, client1);
        assertEquals(client1, ts.getCid());
        assertEquals(n, ts.getNum());

        // inc
        ts.inc(client2);
        assertEquals(client2, ts.getCid());
        assertEquals(++n, ts.getNum());

        // parse ts string
        String tsStr = "42_dontpanic";
        Timestamp tsp = Timestamp.parseString(tsStr);
        assertEquals(42, tsp.getNum());
        assertEquals("dontpanic", tsp.getCid());

        // from toString to parse
        n = this.random.nextInt(100);
        client1 = Utils.generateClientId();
        ts = new Timestamp(n, client1);
        Timestamp tsp1 = Timestamp.parseString(ts.toString());
        assertEquals(ts, tsp1);
        assertEquals(client1, tsp1.getCid());
        assertEquals(n, tsp1.getNum());
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
}
