import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class Worker {

    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

    private final String serverId = Integer.toHexString(new Random().nextInt());
    private final String hostPort;

    private ZooKeeper zk;
    private String status;

    Worker(String hostport) {
        this.hostPort = hostport;
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
     * Register me under /workers/worker-${serverId}
     */
    void register() {
        zk.create("/workers/worker-" + serverId,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                registerCallback,
                null);
    }

    private AsyncCallback.StringCallback registerCallback = (rc, path, ctx, name) -> {
        KeeperException.Code code = KeeperException.Code.get(rc);
        switch (code) {
            case CONNECTIONLOSS:
                register();
                break;
            case OK:
                LOG.info(">>> Registered successfully {}", serverId);
                break;
            default:
                LOG.error(">>> Something went wrong while registering", KeeperException.create(code, path));
        }
    };

    /** Set my status */
    void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    private synchronized void updateStatus(String status) {
        // Extra check to ignore callback retries that go after another update,
        // we want setStatus to be last-write-wins
        if (status.equals(this.status)) {
            zk.setData("/workers/worker-" + serverId, status.getBytes(), -1, updateStatusCallback, status);
        }
    }

    private AsyncCallback.StatCallback updateStatusCallback = (rc, path, ctx, stat) -> {
        if (KeeperException.Code.get(rc) == KeeperException.Code.CONNECTIONLOSS) {
            updateStatus((String) ctx);
        }
    };

    public static void main(String[] args) throws IOException, InterruptedException {
        Worker w = new Worker(args[0]);
        try {
            w.connect();
            w.register();
            Thread.sleep(30000);
        } finally {
            w.disconnect();
        }
    }
}
