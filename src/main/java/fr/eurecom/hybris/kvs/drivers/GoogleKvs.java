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
package fr.eurecom.hybris.kvs.drivers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpStatus;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSBucket;
import org.jets3t.service.model.GSObject;
import org.jets3t.service.security.GSCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

import fr.eurecom.hybris.Config;

public class GoogleKvs extends Kvs {

    private static final Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private transient final GoogleStorageService gsService;

    public GoogleKvs(String id, String accessKey, String secretKey,
            String container, boolean enabled, int cost) throws IOException {
        super(id, container, enabled, cost);

        GSCredentials gsCredentials = new GSCredentials(accessKey, secretKey);
        try {
            this.gsService = new GoogleStorageService(gsCredentials);
        } catch (ServiceException e) {
            logger.error("Could not initialize {} KvStore", id, e);
            throw new IOException(e);
        }

        this.createContainer();
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

    private void createContainer() throws IOException {
        try {
            this.gsService.createBucket(this.rootContainer, GSBucket.LOCATION_EUROPE, null);	// XXX hardcoded bucket location 
        } catch (ServiceException e) {
        	/* In case the bucket is already existing, we get this exception:
        	 * 	ResponseCode: 409
        	 * 	ResponseStatus: Conflict
        	 */
        	if (e.getResponseCode() != 409)
        		throw new IOException(e);
        }
    }

    public void shutdown() throws IOException {
        try {
            this.gsService.shutdown();
        } catch (ServiceException e) {
            throw new IOException(e);
        }
    }
}
