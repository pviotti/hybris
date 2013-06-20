package fr.eurecom.hybris;

import java.io.FileInputStream;
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
    private static String accountsConfFileName = "accounts.properties";
    private static String log4jConfFileName = "log4j.properties";
    
    public static String LOGGER_NAME = "hybrisLogger";
    
    public static String CONST_T = "fr.eurecom.hybris.t";
    
    public static String ZK_ADDR = "fr.eurecom.hybris.zk.address";
    public static String ZK_ROOT = "fr.eurecom.hybris.zk.root";
    
    public static String KVS_ROOT = "fr.eurecom.hybris.kvs.root";
    
    private static String C_ACCOUNTS = "fr.eurecom.hybris.clouds";
    public static String C_AKEY = "fr.eurecom.hybris.clouds.%s.akey";
    public static String C_SKEY = "fr.eurecom.hybris.clouds.%s.skey";
    public static String C_ENABLED = "fr.eurecom.hybris.clouds.%s.enabled";
    public static String C_COST = "fr.eurecom.hybris.clouds.%s.cost";
    
    private Config () {
        try {
            hybrisProperties = new Properties();
            hybrisProperties.load(new FileInputStream(generalConfFileName));
            
            accountsProperties = new Properties();
            accountsProperties.load(new FileInputStream(accountsConfFileName));
            
            PropertyConfigurator.configure(log4jConfFileName);
        } catch (Exception e) {
            System.err.println("FATAL: Could not find properties and/or log4j configuration files.");
            System.exit(-1);
        }
    }
    
    public static Config getInstance () {
        if (instance == null)
            instance = new Config();
        return instance;
    }
    
    public String getProperty (String key) {
        return hybrisProperties.getProperty(key);
    }
       
    public String[] getAccountsIds() {
        return accountsProperties.getProperty(C_ACCOUNTS).split(",");
    }
    
    public String getAccountsProperty(String key) {
        return accountsProperties.getProperty(key);
    }
}
