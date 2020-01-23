import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Master {

    private ZooKeeper zk;
    private final String hostPort;

    public Master(String hostport) {
        this.hostPort = hostport;
    }

    public void connect() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, System.out::println);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Master m = new Master(args[0]);
        m.connect();
        Thread.sleep(60000);
    }
}
