package fr.eurecom.hybris;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {
    
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
}
