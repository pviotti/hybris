package fr.eurecom.hybris;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import fr.eurecom.hybris.mds.Metadata.Timestamp;

public class Utils {
    
    private static String clientId = null;
    private static String KVS_KEY_SEPARATOR = "#";
    
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
    
    public static String getClientId() {
        
        if (clientId == null)
            try {
                clientId = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                clientId = UUID.randomUUID().toString()
                                .replace("-", "").substring(0, 10);
            }
        return clientId;
    }
    
    public static String getKvsKey(String key, Timestamp ts) {
        return key + KVS_KEY_SEPARATOR + ts;
    }
}
