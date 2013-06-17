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
    
    private static String generalConfFileName = "hybris.properties";
    private static String log4jConfFileName = "log4j.properties";
    
    public static String LOGGER_NAME = "hybrisLogger";
            
    public static String ZK_ADDR = "fr.eurecom.hybris.zk.address";
    public static String ZK_ROOT = "fr.eurecom.hybris.zk.root";
    
    private Config () {
	    try {
	    	hybrisProperties = new Properties();
	    	hybrisProperties.load(new FileInputStream(generalConfFileName));
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
}
