package fr.eurecom.hybris;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

/**
 * Singleton class for retrieving 
 * configurations properties.
 * @author p.viotti
 */
public class Config {
    
    private static Config instance;
    private Properties hybrisProperties;
    private Properties accountsProperties;
    
    private static String generalConfFileName = "hybris.properties";
    private static String log4jConfFileName = "log4j.properties";
    
    public static String LOGGER_NAME = "hybrisLogger";
    
    public static String HS_T = "fr.eurecom.hybris.t";
    public static String HS_TO_WRITE = "fr.eurecom.hybris.timeoutwrite";
    public static String HS_TO_READ = "fr.eurecom.hybris.timeoutread";
    public static String HS_GC = "fr.eurecom.hybris.gc";
    
    public static String ZK_ADDR = "fr.eurecom.hybris.zk.address";
    public static String ZK_ROOT = "fr.eurecom.hybris.zk.root";
    
    public static String KVS_ROOT = "fr.eurecom.hybris.kvs.root";
    public static String KVS_ACCOUNTSFILE = "fr.eurecom.hybris.kvs.accountsfile";
    public static String KVS_TESTSONSTARTUP = "fr.eurecom.hybris.kvs.latencytestonstartup";
    
    private static String C_ACCOUNTS = "fr.eurecom.hybris.clouds";
    public static String C_AKEY = "fr.eurecom.hybris.clouds.%s.akey";
    public static String C_SKEY = "fr.eurecom.hybris.clouds.%s.skey";
    public static String C_ENABLED = "fr.eurecom.hybris.clouds.%s.enabled";
    public static String C_COST = "fr.eurecom.hybris.clouds.%s.cost";
    
    private Config () throws IOException {
        try {
            hybrisProperties = new Properties();
            hybrisProperties.load(new FileInputStream(generalConfFileName));
            PropertyConfigurator.configure(log4jConfFileName);
        } catch (IOException e) {
            System.err.println("FATAL: Could not find properties and/or log4j configuration files.");
            throw e;
        }
    }
    
    public static Config getInstance () throws IOException {
        if (instance == null)
            instance = new Config();
        return instance;
    }
    
    public String getProperty (String key) {
        return hybrisProperties.getProperty(key);
    }
    
    /* --------------- Accounts properties management --------------- */ 
    
    public void loadAccountsProperties() throws IOException {
        accountsProperties = new Properties();
        accountsProperties.load(new FileInputStream(hybrisProperties.getProperty(KVS_ACCOUNTSFILE)));
    }
    
    public void loadAccountsProperties(String propertiesFile) throws IOException {
        accountsProperties = new Properties();
        accountsProperties.load(new FileInputStream(propertiesFile));
    }
    
    public String[] getAccountsIds() throws IOException {
        if (accountsProperties == null)
            loadAccountsProperties();
        return accountsProperties.getProperty(C_ACCOUNTS).trim().split(",");
    }
    
    public String getAccountsProperty(String key) throws IOException {
        if (accountsProperties == null)
            loadAccountsProperties();
        return accountsProperties.getProperty(key);
    }
}