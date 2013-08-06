package fr.eurecom.hybris.kvs.drivers;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.eurecom.hybris.Config;

public class AzureKvs extends Kvs {

    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    public AzureKvs(String id, String accessKey, String secretKey,
                            String container, boolean enabled, int cost) {
        super(id, accessKey, secretKey, container, enabled, cost);
        // TODO Auto-generated constructor stub
    }

    public void put(String key, byte[] value) {
        // TODO Auto-generated method stub

    }

    public byte[] get(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> list() {
        // TODO Auto-generated method stub
        return null;
    }

    public void delete(String key) {
        // TODO Auto-generated method stub

    }

    public boolean createContainer() throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    public void emptyContainer() {
        // TODO Auto-generated method stub

    }
}
