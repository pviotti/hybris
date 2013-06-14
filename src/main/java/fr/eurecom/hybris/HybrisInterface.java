package fr.eurecom.hybris;

public interface HybrisInterface {
	
	public byte[] read(String key);
	
	public void write(String key, byte[] value);
	
	public void gc();

}
