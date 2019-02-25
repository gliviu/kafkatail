package kt.test;

import org.assertj.core.api.Assertions;
import org.awaitility.Duration;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static kt.test.TestUtils.bootstrapServers;
import static kt.test.TestUtils.isServerAvailable;
import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

/**
 * Runs kafka server before starting the first test.
 */
public class EmbeddedKafkaJunitExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

    private static AtomicBoolean started = new AtomicBoolean();
    private static EmbeddedKafka kafka;

    @Override
    public void beforeAll(ExtensionContext context) {
        if (started.compareAndSet(false, true)) {
            Properties kafkaProperties = new Properties();
            Properties zkProperties = new Properties();
            try {
                //load properties
                kafkaProperties.load(EmbeddedKafkaJunitExtension.class.getResourceAsStream("/kafka.properties"));
                Path kafkaLogDir = Files.createTempDirectory("kafka-logs");
                kafkaProperties.setProperty("log.dirs", kafkaLogDir.toString());
                zkProperties.load(EmbeddedKafkaJunitExtension.class.getResourceAsStream("/zoo.cfg"));
                Path zkDataDir = Files.createTempDirectory("zookeeper-data");
                zkProperties.setProperty("dataDir", zkDataDir.toString());

                //start kafka
                kafka = new EmbeddedKafka(kafkaProperties, zkProperties);
                Assertions.assertThat(isServerAvailable(Duration.TEN_SECONDS))
                        .as("Kafka server did not start - " + bootstrapServers())
                        .isTrue();

                // init producer
                TestProducer.init(bootstrapServers());
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            context.getRoot().getStore(GLOBAL).put(UUID.randomUUID().toString(), this);
        }
    }

    @Override
    public void close() {
        TestProducer.close();
        kafka.stop();
    }
}