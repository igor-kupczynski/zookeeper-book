import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

/**
 * Provide admin view of master-worker system state
 */
public class Admin {

    private static final Logger LOG = LoggerFactory.getLogger(Admin.class);

    private final String hostPort;

    private ZooKeeper zk;

    public Admin(String hostPort) {
        this.hostPort = hostPort;
    }

    /**
     * Connect to zookeeper server
     */
    void connect() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, ev -> LOG.info(">>> Received event {}", ev));
    }

    /**
     * Disconnect from zookeeper server
     */
    void disconnect() throws InterruptedException {
        zk.close();
    }

    /**
     * Show the current system state
     */
    void showState() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: " + new String(masterData) +
                    " since " + startDate);
        } catch (KeeperException.NoNodeException e) {
            System.out.println("No Master");
        }

        System.out.println("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte[] data = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + ": " + state);
        }

        System.out.println("Assignments:");
        for (String t : zk.getChildren("/assign", false)) {
            System.out.println("\t" + t);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Admin a = new Admin(args[0]);
        try {
            a.connect();
            a.showState();
        } catch (KeeperException | IOException e) {
            LOG.error("Can't show system state", e);
        } finally {
            a.disconnect();
        }
    }
}
