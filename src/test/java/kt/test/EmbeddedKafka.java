package kt.test;

import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * https://gist.github.com/fjavieralba/7930018
 */
public class EmbeddedKafka {
    private final static Logger logger = LoggerFactory.getLogger(EmbeddedKafka.class);
    public KafkaServerStartable kafka;
    public EmbeddedZookeeper zookeeper;

    public EmbeddedKafka(Properties kafkaProperties, Properties zkProperties) {
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

        //start local zookeeper
        logger.info("Starting embedded zookeeper...");
        zookeeper = new EmbeddedZookeeper(zkProperties);

        //start local kafka broker
        kafka = new KafkaServerStartable(kafkaConfig);
        logger.info("Starting embedded kafka broker...");
        kafka.startup();
        System.out.println("\n");
    }


    public void stop() {
        //stop kafka broker
        logger.info("Stopping embedded kafka broker...");
        kafka.shutdown();
    }

}