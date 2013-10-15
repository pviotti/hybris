package fr.eurecom.hybris;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import javax.xml.bind.DatatypeConverter;

import fr.eurecom.hybris.mds.Metadata.Timestamp;

public class Utils {

    private static String KVS_KEY_SEPARATOR = "#";

    public static int HASH_LENGTH = 20; // length of hash digest

    public static byte[] getHash(byte[] inputBytes) {
        MessageDigest hash;
        try {
            hash = MessageDigest.getInstance("SHA-1");
            hash.reset();
            hash.update(inputBytes);
            return hash.digest();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String bytesToHexStr(byte[] array) {
        if (array == null)
            return null;
        else
            return DatatypeConverter.printHexBinary(array);
    }

    public static byte[] hexStrToBytes(String s) {
        if (s == null)
            return null;
        else
            return DatatypeConverter.parseHexBinary(s);
    }

    public static String generateClientId() {

        // TODO
        //        if (this.clientId == null)
        //            try {
        //                clientId = InetAddress.getLocalHost().getHostName();
        //            } catch (UnknownHostException e) {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        //            }
        //        return this.clientId;
    }

    public static String getKvsKey(String key, Timestamp ts) {
        return key + KVS_KEY_SEPARATOR + ts;
    }

    public static String getKeyFromKvsKey(String kvsKey) {
        return kvsKey.split(KVS_KEY_SEPARATOR)[0];
    }

    public static Timestamp getTimestampfromKvsKey(String kvsKey) {
        String tsStr = kvsKey.split(KVS_KEY_SEPARATOR)[1];
        return Timestamp.parseString(tsStr);
    }
}
