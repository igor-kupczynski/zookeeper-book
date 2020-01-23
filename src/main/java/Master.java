import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

public class Master {

    private final String serverId;
    private final String hostPort;

    private ZooKeeper zk;
    private boolean isMaster = false;

    Master(String hostport) {
        this.serverId = Integer.toHexString(new Random().nextInt());
        this.hostPort = hostport;
    }

    /**
     * Connect to zookeeper server
     */
    void connect() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, System.out::println);
    }

    /**
     * Disconnect from zookeeper server
     */
    void disconnect() throws InterruptedException {
        zk.close();
    }

    /**
     * Return true if there is a master or false otherwise.
     * <p>
     * Side-effect: if there's a master, then reset this.isMaster; isMaster reset to true indicates that we are
     * the master, false indicates that some other server is.
     */
    boolean checkMaster() throws InterruptedException {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData("/master", false, stat);
                isMaster = new String(data).equals(serverId);
                return true;
            } catch (KeeperException.NoNodeException e) {
                // no master
                return false;
            } catch (KeeperException e) {
                // Possibly KeeperException.ConnectionLossException
                // do nothing and retry
            }
        }
    }


    /**
     * Try to acquire master lock.
     * <p>
     * Side-effect: reset this.isMaster.
     * <p>
     * In case of connection issues will make sure that we are (or we are not) the master before returning. Retrying
     * to gain the mastership if necessary.
     */
    void runForMaster() throws InterruptedException {
        while (true) {
            try {
                zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isMaster = true;
                break;
            } catch (KeeperException.NodeExistsException e) {
                // other master
                isMaster = false;
                break;
            } catch (KeeperException e) {
                // Possibly KeeperException.ConnectionLossException
                // do nothing and carry on
            }
            if (checkMaster()) {
                break;
            }
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        Master m = new Master(args[0]);
        try {
            m.connect();
            m.runForMaster();
            if (m.isMaster) {
                System.out.println(">>> I'm the leader");
                // wait for a bit
                Thread.sleep(60000);
            } else {
                System.out.println(">>> Someone else is the leader");
            }
        } finally {
            m.disconnect();
        }
    }
}
