package fr.eurecom.hybris.kvs.drivers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpStatus;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.io.ByteStreams;

public class AmazonKvs extends Kvs {

    private static final long serialVersionUID = 1L;
    //private transient static Logger logger = LoggerFactory.getLogger(Config.LOGGER_NAME);

    private transient final AmazonS3 s3;

    public AmazonKvs(String id, final String accessKey, final String secretKey,
                            String container, boolean enabled, int cost) {
        super(id, accessKey, secretKey, container, enabled, cost);

        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey,secretKey);
        this.s3 = new AmazonS3Client(credentials);
    }

    public void put(String key, byte[] value) throws IOException {
        try {
            ObjectMetadata om = new ObjectMetadata();
            om.setContentLength(value.length);
            this.s3.putObject(
                    new PutObjectRequest(
                            this.rootContainer, key, new ByteArrayInputStream(value), om));
        } catch (AmazonClientException e) {
            throw new IOException(e);
        }
    }

    public byte[] get(String key) throws IOException {
        try {
            S3Object object = this.s3.getObject(new GetObjectRequest(this.rootContainer, key));
            return ByteStreams.toByteArray(object.getObjectContent());
        } catch (AmazonClientException e) {

            if (e instanceof AmazonS3Exception) {
                AmazonS3Exception as3e = (AmazonS3Exception) e;
                if (as3e.getStatusCode() == HttpStatus.SC_NOT_FOUND)
                    return null;
            }

            throw new IOException(e);
        }
    }

    public void delete(String key) throws IOException {
        try {
            this.s3.deleteObject(this.rootContainer, key);
        } catch (AmazonClientException e) {
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
            throw new IOException(e);
        }
    }

    public void createContainer() throws IOException {
        try {
            this.s3.createBucket(this.rootContainer);   // TODO if the bucket already exists?
            this.alreadyUsed = true;
        } catch (AmazonClientException e) {
            throw new IOException(e);
        }
    }
}
