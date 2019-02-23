package kt.cli;

import org.apache.kafka.clients.admin.*;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;


public class KafkaAdmin {

    public static final Duration LIST_TOPICS_TIMEOUT = Duration.ofSeconds(10);

    public Set<String> allTopics(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient ac = AdminClient.create(props)) {
            ListTopicsOptions options = new ListTopicsOptions().timeoutMs((int) LIST_TOPICS_TIMEOUT.toMillis());
            ListTopicsResult topics = ac.listTopics(options);
            return topics.names().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

}
