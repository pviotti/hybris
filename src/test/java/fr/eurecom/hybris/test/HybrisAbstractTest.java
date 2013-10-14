package fr.eurecom.hybris.test;

import java.security.SecureRandom;
import java.util.Arrays;

import org.apache.curator.test.TestingServer;

abstract class HybrisAbstractTest {

    protected String TEST_KEY_PREFIX = "test-";
    protected SecureRandom random = new SecureRandom();
    protected static TestingServer zkTestingServer;

    protected byte[] generatePayload(int size, byte b) {
        byte[] array = new byte[size];
        Arrays.fill(array, b);
        return array;
    }
}
