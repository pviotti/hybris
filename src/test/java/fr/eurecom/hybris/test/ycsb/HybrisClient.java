package fr.eurecom.hybris.test.ycsb;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;

public class HybrisClient extends DB {
    
    private Hybris hybris;
    
    private String KVS_ROOT = "kvstest-root";
    private String MDS_TEST_ROOT = "mdstest-root";
    private String MDS_ADDRESS = "localhost:2181";
    
    private String KVS_ACCOUNTS_FILE = "/home/paolo/workspace/hybris/accounts-test.properties";
    
    public void init() throws DBException {
        
        try {
            hybris = new Hybris(MDS_ADDRESS, MDS_TEST_ROOT, KVS_ACCOUNTS_FILE, KVS_ROOT, false, 0, 600, 600, true);
        } catch (HybrisException e) {
            e.printStackTrace();
            throw new DBException(e);
        }
    }

    public void cleanup() throws DBException {
    }

    
    
    @Override
    public int read(String table, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {
        
        System.out.println("Hybris read...");
        try {
            byte[] value = hybris.read(key);
            if (value == null)
                return 1;
            else
                return 0;
        } catch (HybrisException e) {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int update(String table, String key,
                        HashMap<String, ByteIterator> values) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int insert(String table, String key,
                        HashMap<String, ByteIterator> values) {
        
        System.out.println("Hybris insert...");
        ByteBuffer target = ByteBuffer.allocate(99999);
        for (String vkey : values.keySet())
            target.put(values.get(vkey).toArray());
        
        try {
            hybris.write(key, target.array());
            return 0;
        } catch (HybrisException e) {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int delete(String table, String key) {
        // TODO Auto-generated method stub
        return 0;
    }
    
    /**
     * TODO TEMP
     * @param args
     */
    public static void main( String[] args ) {
        System.out.println( "Hello World!" );
    }
}
