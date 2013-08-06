package fr.eurecom.hybris.kvs.drivers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.io.ByteStreams;

import fr.eurecom.hybris.Config;

public class AmazonKvs extends Kvs {

    private static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private final AmazonS3 s3;

    public AmazonKvs(String id, final String accessKey, final String secretKey,
                            String container, boolean enabled, int cost) {
        super(id, accessKey, secretKey, container, enabled, cost);

        this.s3 = new AmazonS3Client(new AWSCredentials() {
            public String getAWSSecretKey() { return secretKey; }
            public String getAWSAccessKeyId() { return accessKey; }
        });
    }

    public void put(String key, byte[] value) throws IOException {
        try {
            this.s3.putObject(
                    new PutObjectRequest(
                            this.rootContainer, key, new ByteArrayInputStream(value), new ObjectMetadata()));
        } catch (AmazonClientException e) {
            logger.warn("Could not put {}", key, e);
            throw new IOException(e);
        }
    }

    public byte[] get(String key) throws IOException {
        try {
            S3Object object = this.s3.getObject(new GetObjectRequest(this.rootContainer, key));
            return ByteStreams.toByteArray(object.getObjectContent());
        } catch (AmazonClientException e) {
            logger.warn("Could not get {}", key, e);
            throw new IOException(e);
        }
    }

    public void delete(String key) throws IOException {
        try {
            this.s3.deleteObject(this.rootContainer, key);
        } catch (AmazonClientException e) {
            logger.warn("Could not delete {}", key, e);
            throw new IOException(e);
        }
    }

    public List<String> list() throws IOException {
        try {
            List<String> keys = new ArrayList<String>();
            ObjectListing objectListing = this.s3.listObjects(this.rootContainer);
            boolean loop = false;

            do {
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries())
                    keys.add(objectSummary.getKey());

                if (objectListing.isTruncated()) {
                    objectListing = this.s3.listNextBatchOfObjects(objectListing);
                    loop = true;
                } else
                    loop = false;

            } while (loop);

            return keys;
        } catch (AmazonClientException e) {
            logger.warn("Could not list {}", this.rootContainer, e);
            throw new IOException(e);
        }
    }

    public boolean createContainer() throws IOException {
        try {
            this.s3.createBucket(this.rootContainer);   // TODO if the bucket already exists?
            return false;
        } catch (AmazonClientException e) {
            logger.warn("Could not create " + this.rootContainer + " on " + this.id, e);
            throw new IOException(e);
        }
    }

    public void emptyContainer() throws IOException {
        try {
            List<String> keys = this.list();
            for (String key : keys)
                this.delete(key);
        } catch (IOException e) {
            throw new IOException(e);
        }
    }
}
