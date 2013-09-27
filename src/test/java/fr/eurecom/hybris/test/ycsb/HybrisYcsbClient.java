package fr.eurecom.hybris.test.ycsb;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;

public class HybrisYcsbClient extends DB {

    private Hybris hybris;

    private static final String KVS_ROOT = "ycsbtest-root";
    private static final String MDS_TEST_ROOT = "ycsbtest-root";
    private static final String MDS_ADDRESS = "localhost:2181";

    private final String PROP_KVS_ACCOUNTS_FILE = "kvs.accountfile";
    private final String PROP_T = "t";

    private static final boolean CACHE_ENABLED = false;
    private static final String CACHE_ADDRESS = "localhost:11211";
    private static final int CACHE_EXP = 0;

    public void init() throws DBException {

        Properties props = this.getProperties();
        String accountFile = props.getProperty(this.PROP_KVS_ACCOUNTS_FILE);
        int t = Integer.parseInt(props.getProperty(this.PROP_T));
        try {
            this.hybris = new Hybris(MDS_ADDRESS, MDS_TEST_ROOT, accountFile,
                    KVS_ROOT, true, t, 600, 600, false,
                    CACHE_ENABLED, CACHE_ADDRESS, CACHE_EXP);
        } catch (HybrisException e) {
            e.printStackTrace();
            throw new DBException(e);
        }
    }

    public void cleanup() throws DBException {  }

    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {

        try {
            byte[] value = this.hybris.read(key);
            if (value == null)
                return 1;
            else{
                result.put(key, new ByteArrayByteIterator(value));
                return 0;
            }
        } catch (HybrisException e) {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result) {

        List<String> keys;
        try {
            keys = this.hybris.list();
        } catch (HybrisException e) {
            e.printStackTrace();
            return 1;
        }

        HashMap<String, ByteIterator> values;
        for (String key : keys) {
            values = new HashMap<String, ByteIterator>();
            this.read(table, key, fields, values);
            result.add(values);
        }

        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return this.insert(table, key, values);
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (String vkey : values.keySet())
            try {
                baos.write(values.get(vkey).toArray());
            } catch (IOException e1) {
                e1.printStackTrace();
            }

        try {
            this.hybris.write(key, baos.toByteArray());
            return 0;
        } catch (HybrisException e) {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int delete(String table, String key) {

        try {
            this.hybris.delete(key);
            return 0;
        } catch (HybrisException e) {
            e.printStackTrace();
            return 1;
        }
    }
}
