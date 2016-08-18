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
package fr.eurecom.hybris;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.UUID;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import fr.eurecom.hybris.mds.Metadata.Timestamp;

public class Utils {

    /** KVS key separator */
    private final static String KVS_KEY_SEPARATOR = "#";

    /** encryption algorithm */
    private final static String ENC_ALGORITHM = "AES";
    private final static String ENC_ALGORITHM_MODE = "AES/CFB/NoPadding";
    public final static int CRYPTO_KEY_LENGTH = 16; // length of AES key (16,24,32)

    /** hashing algorithm */
    private final static String HASH_ALGORITHM = "SHA-224";
    public final static int HASH_LENGTH = 28;

    private final static SecureRandom random = new SecureRandom();

    public static byte[] getHash(byte[] inputBytes) {
        MessageDigest hash;
        try {
            hash = MessageDigest.getInstance(HASH_ALGORITHM);
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

    public static byte[] generateRandomBytes(byte[] array) {
        random.nextBytes(array);
        return array;
    }


    /* -------------------------------------- Encryption / decryption functions -------------------------------------- */

    /**
     * Encrypts the given plaintext using the supplied encryption key.
     * @param plainValue
     * @param encKey - byte array containing the encryption key
     * @param iv - the initialization vector
     * @return byte[]
     * @throws NoSuchAlgorithmException
     * @throws NoSuchProviderException
     * @throws NoSuchPaddingException
     * @throws InvalidKeyException
     * @throws IllegalBlockSizeException
     * @throws BadPaddingException
     * @throws InvalidAlgorithmParameterException
     */
    public static byte[] encrypt(byte[] plainValue, byte[] encKey, byte[] iv)
            throws NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException,
            InvalidKeyException, IllegalBlockSizeException, BadPaddingException,
            InvalidAlgorithmParameterException {
        Cipher cipher = Cipher.getInstance(ENC_ALGORITHM_MODE);
        SecretKeySpec key = new SecretKeySpec(encKey, ENC_ALGORITHM);
        IvParameterSpec ivSpec = new IvParameterSpec(iv);
        cipher.init(Cipher.ENCRYPT_MODE, key, ivSpec);
        return cipher.doFinal(plainValue);
    }

    /**
     * Decrypts the given ciphertext using the supplied encryption key.
     * @param cipherText
     * @param encKey - byte array containing the encryption key
     * @param iv - the initialization vector
     * @return byte[]
     * @throws NoSuchAlgorithmException
     * @throws NoSuchPaddingException
     * @throws InvalidKeyException
     * @throws UnsupportedEncodingException
     * @throws IllegalBlockSizeException
     * @throws BadPaddingException
     * @throws InvalidAlgorithmParameterException
     */
    public static byte[] decrypt(byte[] cipherText, byte[] encKey, byte[] iv)
            throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException,
            UnsupportedEncodingException, IllegalBlockSizeException, BadPaddingException,
            InvalidAlgorithmParameterException {
        Cipher cipher = Cipher.getInstance(ENC_ALGORITHM_MODE);
        SecretKeySpec key = new SecretKeySpec(encKey, ENC_ALGORITHM);
        IvParameterSpec ivSpec = new IvParameterSpec(iv);
        cipher.init(Cipher.DECRYPT_MODE, key, ivSpec);
        return cipher.doFinal(cipherText);
    }

    /* -------------------------------------- Data compression functions -------------------------------------- */

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
