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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

import fr.eurecom.hybris.Config;

public class AzureKvs extends Kvs {

	private transient static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

	private static final String ERROR_BLOB_NOT_FOUND = "BlobNotFound";

	private transient final CloudBlobClient blobClient;
	private transient CloudBlobContainer containerRef;
	private transient HashMap<String, CloudBlockBlob> blobRefs; // cached
																// references of
																// already used
																// blobs

	public AzureKvs(String id, String accessKey, String secretKey, String container, boolean enabled, int cost)
			throws IOException {
		super(id, container, enabled, cost);

		String storageConnectionString = "DefaultEndpointsProtocol=http;" + "AccountName=" + accessKey + ";AccountKey="
				+ secretKey;
		CloudStorageAccount storageAccount;
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
			this.blobClient = storageAccount.createCloudBlobClient();
			BlobRequestOptions bro = new BlobRequestOptions();
			bro.setSingleBlobPutThresholdInBytes(30000000); // ~30 MB
			bro.setConcurrentRequestCount(200);
			this.blobClient.setDefaultRequestOptions(bro);
		} catch (InvalidKeyException | URISyntaxException e) {
			logger.error("Could not initialize {} KvStore", id, e);
			throw new IOException(e);
		}

		this.createContainer();
		this.blobRefs = new HashMap<String, CloudBlockBlob>();
	}

	public void put(String key, byte[] value) throws IOException {
		try {
			CloudBlockBlob blob = this.containerRef.getBlockBlobReference(key);
			blob.getProperties().setContentMD5(null);
			blob.upload(new ByteArrayInputStream(value), value.length);
			this.blobRefs.put(key, blob);
		} catch (URISyntaxException | StorageException | IOException e) {
			throw new IOException(e);
		}
	}

	public byte[] get(String key) throws IOException {
		try {
			CloudBlockBlob blob = this.blobRefs.get(key);
			if (blob == null)
				blob = this.containerRef.getBlockBlobReference(key);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			blob.download(baos);
			return baos.toByteArray();
		} catch (URISyntaxException | StorageException e) {

			if (e instanceof StorageException) {
				StorageException se = (StorageException) e;
				if (ERROR_BLOB_NOT_FOUND.equals(se.getErrorCode()))
					return null;
			}

			throw new IOException(e);
		}
	}

	public void delete(String key) throws IOException {
		try {
			CloudBlockBlob blob = this.blobRefs.get(key);
			if (blob == null)
				blob = this.containerRef.getBlockBlobReference(key);
			blob.delete();
			this.blobRefs.remove(key);
		} catch (URISyntaxException | StorageException e) {

			if (e instanceof StorageException) {
				StorageException se = (StorageException) e;
				if (ERROR_BLOB_NOT_FOUND.equals(se.getErrorCode()))
					return;
			}

			throw new IOException(e);
		}
	}

	public List<String> list() throws IOException {
		List<String> keys = new ArrayList<String>();
		for (ListBlobItem blobItem : this.containerRef.listBlobs()) { 
			// XXX useFlatBlobListing flag to get only blobs
			// regardless of virtual directories
			CloudBlob blob = (CloudBlob) blobItem;
			keys.add(blob.getName());
		}
		return keys;

	}

	private void createContainer() throws IOException {
		try {
			this.containerRef = this.blobClient.getContainerReference(this.rootContainer);
			this.containerRef.createIfNotExists();
		} catch (StorageException | URISyntaxException e) {
			throw new IOException(e);
		}
	}

	public void shutdown() throws IOException {
	}
}
