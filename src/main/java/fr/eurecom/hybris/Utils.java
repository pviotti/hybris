package fr.eurecom.hybris;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

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
        //InetAddress.getLocalHost().getHostName();
        return UUID.randomUUID().toString().replace("-", "").substring(0, 10);
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

    public static byte[] compress(byte[] data) {    // XXX
        ByteArrayOutputStream baos = null;
        Deflater dfl = new Deflater(Deflater.BEST_COMPRESSION, true);
        dfl.setInput(data);
        dfl.finish();
        baos = new ByteArrayOutputStream();
        byte[] tmp = new byte[4*1024];
        try{
            while(!dfl.finished()){
                int size = dfl.deflate(tmp);
                baos.write(tmp, 0, size);
            }
        } catch (Exception ex){
            ex.printStackTrace();
            return data;
        } finally {
            try{
                if(baos != null) baos.close();
            } catch(Exception ex){}
        }
        return baos.toByteArray();
    }

    public static byte[] decompress(byte[] data) throws IOException, DataFormatException {  // XXX
        Inflater inflater = new Inflater(true);
        inflater.setInput(data);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        byte[] output = outputStream.toByteArray();

        inflater.end();

        return output;
    }
}
