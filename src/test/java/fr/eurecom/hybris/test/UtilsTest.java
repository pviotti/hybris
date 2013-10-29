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
        byte[] key = new byte[16];
        this.random.nextBytes(key);
        byte[] clearText = null, cipherText = null;
        try {
            cipherText = Utils.encrypt(value, key);
            clearText = Utils.decrypt(cipherText, key);
            assertArrayEquals(value, clearText);
        } catch(GeneralSecurityException | UnsupportedEncodingException ge) {
            ge.printStackTrace();
            fail();
        }

        // key length = 192 bit (24 byte)
        key = new byte[24];
        this.random.nextBytes(key);
        clearText = null; cipherText = null;
        try {
            cipherText = Utils.encrypt(value, key);
            clearText = Utils.decrypt(cipherText, key);
            assertArrayEquals(value, clearText);
        } catch(GeneralSecurityException | UnsupportedEncodingException ge) {
            ge.printStackTrace();
            fail();
        }

        // key length = 256 bit (32 byte)
        key = new byte[32];
        this.random.nextBytes(key);
        clearText = null; cipherText = null;
        try {
            cipherText = Utils.encrypt(value, key);
            clearText = Utils.decrypt(cipherText, key);
            assertArrayEquals(value, clearText);
        } catch(GeneralSecurityException | UnsupportedEncodingException ge) {
            ge.printStackTrace();
            fail();
        }

        // wrong key length
        key = new byte[13];
        this.random.nextBytes(key);
        clearText = null; cipherText = null;
        try {
            cipherText = Utils.encrypt(value, key);
            clearText = Utils.decrypt(cipherText, key);
            fail();
            assertArrayEquals(value, clearText);
        } catch(GeneralSecurityException | UnsupportedEncodingException ge) {
        }
    }
}
