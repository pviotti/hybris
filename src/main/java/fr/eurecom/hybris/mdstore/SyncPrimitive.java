package fr.eurecom.hybris.mdstore;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class SyncPrimitive implements Watcher {

    protected static ZooKeeper zk = null;
    static Integer mutex;

    String root;

    protected SyncPrimitive(String address) throws IOException {
        if(zk == null){
            try {
                zk = new ZooKeeper(address, 3000, this);
                mutex = new Integer(-1);
            } catch (IOException e) {
                zk = null;
                System.err.println("FATAL: could not initialize the Zookeeper client");
                throw e;
            }
        }
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            //System.out.println("Process: " + event.getType());
            mutex.notify();
        }
    }
}