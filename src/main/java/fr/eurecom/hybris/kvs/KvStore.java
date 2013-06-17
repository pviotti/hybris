package fr.eurecom.hybris.kvs;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;


public class KvStore {
	
	private HashMap<Object, String[]> storageProviders;
	private HashMap<Object, Integer> firstTimeFlag;
	private String folder;

	public KvStore(String folder) {
		
		this.folder = folder;
		
		// TODO retrieve them from a configuration file
		storageProviders = new HashMap<Object, String[]>();
		storageProviders.put("aws-s3", new String[] { 	System.getenv("s3user"), 
														System.getenv("s3pass") });
		storageProviders.put("cloudfiles-us", new String[] { 	System.getenv("rackspaceuser"),
																System.getenv("rackspacepass") });
		storageProviders.put("azureblob", new String[] { System.getenv("azureuser"),
														System.getenv("azurepass") });
		
		firstTimeFlag = new HashMap<Object, Integer>();	// to check the first time create container
		for (Object provider : storageProviders.keySet())
			firstTimeFlag.put(provider, 0);
	}
	
	public List<Object> put(Object filename, Object value) {
		List<Object> sucessSavedKVSList = new ArrayList<Object>();
		BlobStoreContext context = null;
		for (Object providerKey : storageProviders.keySet()) {
			
			try {
				context = ContextBuilder.newBuilder((String) providerKey)
										.credentials(	storageProviders.get(providerKey)[0],
														storageProviders.get(providerKey)[1])
										.buildView(BlobStoreContext.class);

				BlobStore storage = context.getBlobStore();
				
				if (firstTimeFlag.get(providerKey) == 0) {
					System.out.println("---- creating container ----");
					storage.createContainerInLocation(null, folder);
					System.out.println("---- container created ----");
					firstTimeFlag.put(providerKey, firstTimeFlag.get(providerKey) + 1);
				}
				
				Blob blob = storage.blobBuilder((String) filename)
									.payload(new File((String) value)).build();
				storage.putBlob(folder, blob);
				sucessSavedKVSList.add(providerKey);
				
				System.out.println("list: " + sucessSavedKVSList.toString());

			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				if (context != null) context.close();
			}

		}
		return sucessSavedKVSList;
	}
	
	
//	private Map<Long, Object> orderKvs() {
//		
//		TreeMap<Long, Object> KVSOrderedMap = new TreeMap<Long, Object>();
//		List<Object> kvsList = jcloudkvs.putInKVS("test_blob", "src/common/BucketedKey.java");
//		if (kvsList.isEmpty()) {
//			System.out.println("can not connect to any kvs.check internet or username/password.");
//			return null;
//		}
//		
//		for (Object kvs : kvsList) {
//			long start = System.currentTimeMillis();
//			List<Object> fakekvsList = new ArrayList<Object>();
//			fakekvsList.add(kvs);
//			boolean b = false;
//			try {
//				b = jcloudkvs.getFromKVS("test_blob", testlocalFile, fakekvsList);
//			} catch (NotExistFileInKVSException e) {
//				e.printStackTrace();
//				System.out.println("Concurrency problem happened");
//			} catch (KVSListEmptyException e) {
//				e.printStackTrace();
//				System.out.println("Concurrency problem happened");
//			}
//			long end = System.currentTimeMillis();
//			long duration = end - start;
//			if (b)
//				KVSOrderedMap.put(duration, kvs);
//		}
//		
//		System.out.println("test KVSOrderedMap:::" + KVSOrderedMap.toString());
//		return KVSOrderedMap;
//	}
	
	
	
	public static void main (String[] args){
		KvStore kvs = new KvStore("hybrismytest");
		kvs.put("mykey", "src/main/java/fr/eurecom/hybris/Hybris.java");
	}
}
