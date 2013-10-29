package fr.eurecom.hybris.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;

import org.junit.Test;

import fr.eurecom.hybris.Utils;

public class UtilsTest extends HybrisAbstractTest {

    @Test
    public void testEncryption() {

        byte[] value = new byte[500];
        this.random.nextBytes(value);

        // AES allows 128, 192 or 256 bit key length, that is 16, 24 or 32 byte.
        // With CFB mode of operation, the cipherText has the same length as the plainText.

        // key length = 128 bit (16 byte)
        int keyLength = 16;
        byte[] keyIV = new byte[keyLength + 16];
        this.random.nextBytes(keyIV);
        byte[] clearText = null, cipherText = null;
        try {
            cipherText = Utils.encrypt(value, keyIV);
            clearText = Utils.decrypt(cipherText, keyIV);
            assertArrayEquals(value, clearText);
        } catch(GeneralSecurityException | UnsupportedEncodingException ge) {
            ge.printStackTrace();
            fail();
        }

        // key length = 192 bit (24 byte)
        keyLength = 24;
        keyIV = new byte[keyLength + 16];
        this.random.nextBytes(keyIV);
        clearText = null; cipherText = null;
        try {
            cipherText = Utils.encrypt(value, keyIV);
            clearText = Utils.decrypt(cipherText, keyIV);
            assertArrayEquals(value, clearText);
        } catch(GeneralSecurityException | UnsupportedEncodingException ge) {
            ge.printStackTrace();
            fail();
        }

        // key length = 256 bit (32 byte)
        keyLength = 32;
        keyIV = new byte[keyLength + 16];
        this.random.nextBytes(keyIV);
        clearText = null; cipherText = null;
        try {
            cipherText = Utils.encrypt(value, keyIV);
            clearText = Utils.decrypt(cipherText, keyIV);
            assertArrayEquals(value, clearText);
        } catch(GeneralSecurityException | UnsupportedEncodingException ge) {
            ge.printStackTrace();
            fail();
        }

        // wrong key length
        keyLength = 13;
        keyIV = new byte[keyLength + 16];
        this.random.nextBytes(keyIV);
        clearText = null; cipherText = null;
        try {
            cipherText = Utils.encrypt(value, keyIV);
            clearText = Utils.decrypt(cipherText, keyIV);
            fail();
            assertArrayEquals(value, clearText);
        } catch(GeneralSecurityException | UnsupportedEncodingException ge) {
        }
    }
}
