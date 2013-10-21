package fr.eurecom.hybris.test.mds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.kvs.drivers.Kvs;
import fr.eurecom.hybris.kvs.drivers.TransientKvs;
import fr.eurecom.hybris.mds.Metadata;
import fr.eurecom.hybris.mds.Metadata.Timestamp;
import fr.eurecom.hybris.test.HybrisAbstractTest;

public class MetadataTest extends HybrisAbstractTest {

    public void testSize() throws IOException, DataFormatException {
        String cid = Utils.generateClientId();
        System.out.println(cid.toCharArray().length);
        Timestamp ts = new Timestamp(34, cid);
        byte[] hash = new byte[20];
        this.random.nextBytes(hash);
        List<Kvs> replicas = new ArrayList<Kvs>();
        replicas.add(new TransientKvs("transient", "A-accessKey", "A-secretKey", "container", true, 20));
        replicas.add(new TransientKvs("transient", "B-accessKey", "B-secretKey", "container", true, 20));
        //replicas.add(new TransientKvs("transient", "C-accessKey", "C-secretKey", "container", true, 20));
        Metadata md = new Metadata(ts, hash, 0, replicas);

        //        System.out.println(SerializationUtils.serialize(ts).length);    // 80

        //        System.out.println(md.serialize().length); // 175 w/ 2 replicas; 501 con java standard serialization(w/o implementing writeexternal/readexternal)
        // 158

        /**
         * 501 with standard java serialization
         * 175 implementing write/readExternal interface and writing a list of String for the replica list
         * 158 implementing write/readExternal interface and writing a list of short for the replica list
         * 
         * 120 with Kryo serialization
         * 45 with Kryo serialization implementing the KryoSerializable interface and writing a list of short for the replica list
         */

        //        System.out.println(md.kryoSerialize().length);  // 120 senza implementare kryoExternalizable, 45 con l'implementazione
        //        System.out.println(Metadata.kryoDeserialize(md.kryoSerialize()).equals(md));

        System.out.println(md.serialize().length);
        System.out.println(new Metadata(md.serialize()).equals(md));

    }

    public static void main(String[] args) throws IOException, DataFormatException {
        new MetadataTest().testSize();
    }

}
