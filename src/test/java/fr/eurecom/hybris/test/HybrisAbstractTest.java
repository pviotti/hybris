package fr.eurecom.hybris.test;

import java.security.SecureRandom;

import junit.framework.TestCase;

abstract class HybrisAbstractTest extends TestCase {

    protected String TEST_KEY_PREFIX = "test-";
    protected SecureRandom random = new SecureRandom();
   
}
