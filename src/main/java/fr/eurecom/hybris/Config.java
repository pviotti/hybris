package fr.eurecom.hybris;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Singleton class for retrieving
 * configurations properties.
 * @author p.viotti
 */
public class Config {

    private static Config instance;
    private Properties hybrisProperties;
    private Properties accountsProperties;

    public static String LOGGER_NAME = "hybrisLogger";

    public static String HS_T = "fr.eurecom.hybris.t";
    public static String HS_TO_WRITE = "fr.eurecom.hybris.timeoutwrite";
    public static String HS_TO_READ = "fr.eurecom.hybris.timeoutread";
    public static String HS_GC = "fr.eurecom.hybris.gc";

    public static String MDS_ADDR = "fr.eurecom.hybris.mds.address";
    public static String MDS_ROOT = "fr.eurecom.hybris.mds.root";

    public static String KVS_ROOT = "fr.eurecom.hybris.kvs.root";
    public static String KVS_ACCOUNTSFILE = "fr.eurecom.hybris.kvs.accountsfile";
    public static String KVS_TESTSONSTARTUP = "fr.eurecom.hybris.kvs.latencytestonstartup";

    private static String C_ACCOUNTS = "fr.eurecom.hybris.clouds";
    public static String C_AKEY = "fr.eurecom.hybris.clouds.%s.akey";
    public static String C_SKEY = "fr.eurecom.hybris.clouds.%s.skey";
    public static String C_ENABLED = "fr.eurecom.hybris.clouds.%s.enabled";
    public static String C_COST = "fr.eurecom.hybris.clouds.%s.cost";

    public static Config getInstance () {
        if (instance == null)
            instance = new Config();
        return instance;
    }

    public void loadProperties(String propertiesFile) throws IOException {
        try {
            this.hybrisProperties = new Properties();
            this.hybrisProperties.load(new FileInputStream(propertiesFile));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public String getProperty (String key) {
        return this.hybrisProperties.getProperty(key);
    }

    /* --------------- Accounts properties management --------------- */

    public void loadAccountsProperties(String propertiesFile) throws IOException {
        this.accountsProperties = new Properties();
        this.accountsProperties.load(new FileInputStream(propertiesFile));
    }

    public String[] getAccountsIds() {
        return this.accountsProperties.getProperty(C_ACCOUNTS).trim().split(",");
    }

    public String getAccountsProperty(String key) {
        return this.accountsProperties.getProperty(key);
    }
}