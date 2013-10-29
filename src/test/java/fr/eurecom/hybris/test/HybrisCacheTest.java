package fr.eurecom.hybris.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigInteger;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

import org.apache.curator.test.TestingServer;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import fr.eurecom.hybris.Config;
import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;
import fr.eurecom.hybris.Utils;
import fr.eurecom.hybris.mds.Metadata.Timestamp;


/**
 * Test suite for memcached functionalities within Hybris.
 * NB: this class requires the memcached client to be running
 * locally on the default port (i.e. 11211).
 * @author P. Viotti
 */
public class HybrisCacheTest extends HybrisAbstractTest {

    private static String memAddr = "127.0.0.1:11211";

    private static Hybris hybris;
    private static String clientId = "clientId";
    private static String accountsFile = "accounts-test.properties";
    private static String kvsRoot = "kvsroot";
    private static String mdsRoot = "mdsroot";

    @BeforeClass
    public static void beforeClassSetup() throws Exception {
        Config.getInstance();
        zkTestingServer = new TestingServer();
    }

    @Ignore
    @Test
    public void testCacheOnWrite() throws IOException, HybrisException {
        hybris = new Hybris(zkTestingServer.getConnectString(), mdsRoot, accountsFile,
                kvsRoot, false, clientId, 0, 6000, 6000, false, false, true, memAddr, 5000, "onwrite");
        MemcachedClient cacheClient = new MemcachedClient(new BinaryConnectionFactory(),
                AddrUtil.getAddresses(memAddr));

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value = new byte[50];
        this.random.nextBytes(value);
        hybris.put(key, value);

        byte[] cached = (byte[]) cacheClient.get(Utils.getKvsKey(key, new Timestamp(0, clientId)));
        assertArrayEquals(value, cached);

        byte[] retrieved = hybris.get(key);
        assertArrayEquals(value, retrieved);
    }

    @Ignore
    @Test
    public void testCacheOnRead() throws IOException, HybrisException {
        hybris = new Hybris(zkTestingServer.getConnectString(), mdsRoot, accountsFile,
                kvsRoot, false, clientId, 0, 6000, 6000, false, false, true, memAddr, 5000, "onread");
        MemcachedClient cacheClient = new MemcachedClient(new BinaryConnectionFactory(),
                AddrUtil.getAddresses(memAddr));

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value = new byte[50];
        this.random.nextBytes(value);
        hybris.put(key, value);

        Object cached = cacheClient.get(Utils.getKvsKey(key, new Timestamp(0, clientId)));
        assertNull(cached);

        byte[] retrieved = hybris.get(key);
        assertArrayEquals(value, retrieved);

        cached = cacheClient.get(Utils.getKvsKey(key, new Timestamp(0, clientId)));
        assertArrayEquals(value, (byte[]) cached);
    }

    @Ignore
    @Test
    public void testTamperedCache() throws IOException, HybrisException {
        hybris = new Hybris(zkTestingServer.getConnectString(), mdsRoot, accountsFile,
                kvsRoot, false, clientId, 0, 6000, 6000, false, false, true, memAddr, 5000, "onwrite");
        MemcachedClient cacheClient = new MemcachedClient(new BinaryConnectionFactory(),
                AddrUtil.getAddresses(memAddr));

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value = new byte[50];
        this.random.nextBytes(value);
        hybris.put(key, value);

        String kvsKey = Utils.getKvsKey(key, new Timestamp(0, clientId));

        byte[] cached = (byte[]) cacheClient.get(kvsKey);
        assertArrayEquals(value, cached);

        byte[] tamperedValue = new byte[50];
        this.random.nextBytes(tamperedValue);
        cacheClient.set(kvsKey, 43200, tamperedValue);

        cached = (byte[]) cacheClient.get(kvsKey);
        assertArrayEquals(tamperedValue, cached);

        byte[] retrieved = hybris.get(key);
        assertArrayEquals(value, retrieved);
    }

    @Ignore
    @Test
    public void testBigFileCache() throws IOException, HybrisException {
        hybris = new Hybris(zkTestingServer.getConnectString(), mdsRoot, accountsFile,
                kvsRoot, false, clientId, 0, 6000, 6000, false, false, true, memAddr, 5000, "onwrite");
        MemcachedClient cacheClient = new MemcachedClient(new BinaryConnectionFactory(),
                AddrUtil.getAddresses(memAddr));

        String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
        byte[] value = this.generatePayload(1000000, (byte) 'x');       // 1MB; depends on memcached settings
        this.random.nextBytes(value);
        hybris.put(key, value);

        byte[] cached = (byte[]) cacheClient.get(Utils.getKvsKey(key, new Timestamp(0, clientId)));
        assertArrayEquals(value, cached);

        byte[] retrieved = hybris.get(key);
        assertArrayEquals(value, retrieved);
    }

    @Test
    public void testMemcachedNotAvailable() {
        try {
            hybris = new Hybris(zkTestingServer.getConnectString(), mdsRoot, accountsFile,
                    kvsRoot, false, clientId, 0, 6000, 6000, false, false, true, "notexistent:1234", 5000, "onwrite");
            String key = this.TEST_KEY_PREFIX + new BigInteger(50, this.random).toString(32);
            byte[] value = new byte[50];
            this.random.nextBytes(value);
            hybris.put(key, value);
            byte[] retrieved = hybris.get(key);
            assertArrayEquals(value, retrieved);
        } catch (HybrisException e) {
            fail();
        }
    }
}
