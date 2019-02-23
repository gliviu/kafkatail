package kt.test;

import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * https://gist.github.com/fjavieralba/7930018
 */
public class EmbeddedZookeeper {
    private final static Logger logger = LoggerFactory.getLogger(EmbeddedZookeeper.class);
    private ZooKeeperServerMain zooKeeperServer;

    public EmbeddedZookeeper(Properties zkProperties) {
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(zkProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        zooKeeperServer = new ZooKeeperServerMain();
        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);


        new Thread(() -> {
            try {
                zooKeeperServer.runFromConfig(configuration);
            } catch (IOException e) {
                logger.error("Zookeeper failed", e);
                e.printStackTrace(System.err);
            }
        }).start();
    }
}