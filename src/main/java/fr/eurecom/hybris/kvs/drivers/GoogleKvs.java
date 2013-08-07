package fr.eurecom.hybris.kvs.drivers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpStatus;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSObject;
import org.jets3t.service.security.GSCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

import fr.eurecom.hybris.Config;

public class GoogleKvs extends Kvs {

    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private transient final GoogleStorageService gsService;

    public GoogleKvs(String id, String accessKey, String secretKey,
                         String container, boolean enabled, int cost) throws IOException {
        super(id, accessKey, secretKey, container, enabled, cost);

        GSCredentials gsCredentials = new GSCredentials(accessKey, secretKey);
        try {
            this.gsService = new GoogleStorageService(gsCredentials);
        } catch (ServiceException e) {
            logger.error("Could not initialize {} KvStore", id, e);
            throw new IOException(e);
        }
    }

    public void put(String key, byte[] value) throws IOException {
        try {
            GSObject object = new GSObject(key);
            ByteArrayInputStream in = new ByteArrayInputStream(value);
            object.setDataInputStream(in);
            object.setContentLength(value.length);
            this.gsService.putObject(this.rootContainer, object);
        } catch (ServiceException e) {
            throw new IOException(e);
        }
    }

    public byte[] get(String key) throws IOException {
        try {
            GSObject objectComplete = this.gsService.getObject(this.rootContainer, key);
            InputStream ins = objectComplete.getDataInputStream();
            return  ByteStreams.toByteArray(ins);
        } catch (ServiceException | IOException e) {

            if (e instanceof ServiceException) {
                ServiceException se = (ServiceException) e;
                if (se.getResponseCode() == HttpStatus.SC_NOT_FOUND)
                    return null;
            }

            throw new IOException(e);
        }
    }

    public void delete(String key) throws IOException {
        try {
            this.gsService.deleteObject(this.rootContainer, key);
        } catch (ServiceException e) {

            if (e instanceof ServiceException) {
                ServiceException se = e;
                if (se.getResponseCode() == HttpStatus.SC_NOT_FOUND)
                    return;
            }

            throw new IOException(e);
        }
    }

    public List<String> list() throws IOException {
        try {
            List<String> keys = new ArrayList<String>();
            GSObject[] objs = this.gsService.listObjects(this.rootContainer);

            for(GSObject obj: objs)
                keys.add(obj.getName());

            return keys;
        } catch (ServiceException e) {
            throw new IOException(e);
        }
    }

    public void createContainer() throws IOException {
        try {
            this.gsService.getOrCreateBucket(this.rootContainer);
            this.alreadyUsed = true;
        } catch (ServiceException e) {
            throw new IOException(e);
        }
    }
}
