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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Singleton class for retrieving
 * configurations properties.
 * @author P. Viotti
 */
public class Config {

    private static Config instance = null;
    private static Properties hybrisProperties = null;
    private static Properties accountsProperties = null;

    public static final String LOGGER_NAME = "hybrisLogger";

    public static final String HS_F = "hybris.f";
    public static final String HS_CLIENTID = "hybris.clientid";
    public static final String HS_TO_WRITE = "hybris.timeoutwrite";
    public static final String HS_TO_READ = "hybris.timeoutread";
    public static final String HS_GC = "hybris.gc";

    public static final String HS_CRYPTO = "hybris.crypto";
    
    public static final String ECODING = "hybris.erasurecoding";
    public static final String ECODING_K = "hybris.erasurecoding.k";

    public static final String CACHE_ENABLED = "hybris.cache";
    public static final String CACHE_ADDRESS = "hybris.cache.address";
    public static final String CACHE_EXP = "hybris.cache.exp";
    public static final String CACHE_POLICY = "hybris.cache.policy";

    public static final String MDS = "hybris.mds";
    public static final String MDS_ADDR = "hybris.mds.address";
    public static final String MDS_ROOT = "hybris.mds.root";
    public static final String MDS_READ = "hybris.mds.quorumread";

    public static final String KVS_ROOT = "hybris.kvs.root";
    public static final String KVS_ACCOUNTSFILE = "hybris.kvs.accountsfile";
    public static final String KVS_TESTSONSTARTUP = "hybris.kvs.latencytestonstartup";

    private static final String C_ACCOUNTS = "hybris.kvs.drivers";
    public static final String C_AKEY = "hybris.kvs.drivers.%s.akey";
    public static final String C_SKEY = "hybris.kvs.drivers.%s.skey";
    public static final String C_ENABLED = "hybris.kvs.drivers.%s.enabled";
    public static final String C_COST = "hybris.kvs.drivers.%s.cost";

    public static synchronized Config getInstance () {
        if (instance == null)
            instance = new Config();
        return instance;
    }

    public synchronized void loadProperties(String propertiesFile) throws IOException {
        if (hybrisProperties == null)
            try {
                propertiesFile = propertiesFile.replaceFirst("^~",System.getProperty("user.home"));
                hybrisProperties = new Properties();
                hybrisProperties.load(new FileInputStream(propertiesFile));
            } catch (Exception e) {
                throw new IOException(e);
            }
    }

    public String getProperty (String key) {
        return hybrisProperties.getProperty(key);
    }

    /* --------------- Accounts properties management --------------- */

    public synchronized void loadAccountsProperties(String propertiesFile) throws IOException {
        if (accountsProperties == null)
            try {
                propertiesFile = propertiesFile.replaceFirst("^~",System.getProperty("user.home"));
                accountsProperties = new Properties();
                accountsProperties.load(new FileInputStream(propertiesFile));
            } catch (Exception e) {
                throw new IOException(e);
            }
    }

    public String[] getAccountsIds() {
        return accountsProperties.getProperty(C_ACCOUNTS).trim().split(",");
    }

    public String getAccountsProperty(String key) {
        return accountsProperties.getProperty(key);
    }
}
