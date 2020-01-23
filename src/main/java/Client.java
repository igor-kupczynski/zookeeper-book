import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Client {

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private final String hostPort;

    private ZooKeeper zk;

    public Client(String hostPort) {
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
     * Add a task to the system state
     */
    String queueCommand(String command) throws KeeperException, InterruptedException {
        while (true) {
            try {
                return zk.create("/tasks/task-",
                        command.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL
                );
            } catch (KeeperException.ConnectionLossException e) {
                // Do nothing and retry
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Client c = new Client(args[0]);
        try {
            c.connect();
            String name = c.queueCommand(args[1]);
            LOG.info("Created " + name);
        } catch (KeeperException e) {
            LOG.error("Error creating znode", e);
        } finally {
            c.disconnect();
        }
    }
}
