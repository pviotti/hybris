package fr.eurecom.hibrys.test;

import junit.framework.TestCase;
import fr.eurecom.hybris.mdstore.MdStore;
import fr.eurecom.hybris.mdstore.TsDir;

public class MdStoreTest extends TestCase {

    public static void main(String[] args) {
        MdStore mds;
        TsDir tsdir;
        int clientID = 100;
        
//        mds = new MdStore("127.0.0.1:2181","/MDSdir");
//        mds.tsWrite(new TsDir(1,"1",null));
        /*tsdir = mds.tsRead("key");
        System.out.println("Version: " + tsdir.getTs().getNum() + " " + tsdir.getTs().getCid());
        tsdir.getTs().inc("100");
        mds.write(tsdir, new byte[4]);
        mds.read(tsdir);*/
    }
}
