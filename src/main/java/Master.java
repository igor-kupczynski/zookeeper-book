import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Random;

public class Master {

    private final String serverId = Integer.toHexString(new Random().nextInt());
    private final String hostPort;

    private ZooKeeper zk;
    private boolean isMaster = false;

    Master(String hostport) {
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
     * Find out if there's an existing master. If not then run for mastership.
     * <p>
     * Side-effect: if there's a master, then reset this.isMaster; isMaster reset to true indicates that we are
     * the master, false indicates that some other server is.
     * <p>
     * In case of connection issues we retry indefinitely.
     */
    void checkMaster() {
        zk.getData("/master", false, checkMasterCallback, null);
    }

    private AsyncCallback.DataCallback checkMasterCallback = (rc, path, ctx, data, stat) -> {
        switch (KeeperException.Code.get(rc)) {
            case OK:
                isMaster = new String(data).equals(serverId);
            case NONODE:
                runForMaster();
            default:
                // Possibly a connection loss
                checkMaster();
        }
    };


    /**
     * Try to acquire master lock.
     * <p>
     * Side-effect: reset this.isMaster.
     * <p>
     * In case of connection issues will make sure that we are (or we are not) the master before returning. Retrying
     * to gain the mastership if necessary.
     */
    void runForMaster() {
        zk.create(
                "/master",
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                runForMasterCallback,
                null
        );
    }

    private final AsyncCallback.StringCallback runForMasterCallback = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case OK:
                isMaster = true;
                break;
            case NODEEXISTS:
                isMaster = false;
                break;
            default:
                // Possibly a connection loss
                checkMaster();
                return;
        }
        System.out.println(">>> I'm " + (isMaster ? "" : "not ") + "the leader");
    };


    public static void main(String[] args) throws IOException, InterruptedException {
        Master m = new Master(args[0]);
        try {
            m.connect();
            m.runForMaster();
            // wait for a bit
            Thread.sleep(60000);
        } finally {
            m.disconnect();
        }
    }
}
