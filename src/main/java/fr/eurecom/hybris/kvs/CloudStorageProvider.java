package fr.eurecom.hybris.kvs;

/**
 * Class for storing cloud storage provider details.
 * @author p.viotti
 */
public class CloudStorageProvider implements Comparable<CloudStorageProvider> {
	
	private String name;
	
	private String accountName;
	private String accessKey;
	
	private boolean alreadyUsed;
	
	private long writeLatency;

	public String getName() 			{ return name; }
	public void setName(String name) 	{ this.name = name; }

	public String getAccountName() 					{ return accountName; }
	public void setAccountName(String accountName) 	{ this.accountName = accountName; }

	public String getAccessKey() 				{ return accessKey; }
	public void setAccessKey(String accessKey) 	{this.accessKey = accessKey;	}

	public boolean isAlreadyUsed() 					{ return alreadyUsed; }
	public void setAlreadyUsed(boolean alreadyUsed) { this.alreadyUsed = alreadyUsed; }
	
	public long getWriteLatency() 					{ return writeLatency; }
	public void setWriteLatency(long writeLatency) 	{ this.writeLatency = writeLatency; }
	
	@Override
	public int compareTo(CloudStorageProvider o) {
		if 	(this.getWriteLatency() < o.getWriteLatency())		
			return -1;
		else if (this.getWriteLatency() > o.getWriteLatency())
			return 1;
		else
			return 0;
	}
}
