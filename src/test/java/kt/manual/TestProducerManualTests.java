package kt.manual;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TestProducerManualTests {

    private static final double MAX_SECONDS = 0.1;

    public static void main(String[] args) {
        Properties props = new Properties();
        String bootstrapServers = "localhost:9092";
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        List<String> topics = new ArrayList<>();
        int TOPIC_NO = 10;
        int PARTITION_NO = 3;
        for (int i = 0; i < TOPIC_NO; i++) {
            topics.add(createTopic("topic" + i, PARTITION_NO, bootstrapServers));
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; ; i++) {
                sleep((int) ((Math.random() * MAX_SECONDS)*1000));
                String key = "key" + i;
                String value = "val" + i;
                producer.send(new ProducerRecord<>(
                        topics.get((int) (Math.random() * TOPIC_NO)),
                        (int) (Math.random() * PARTITION_NO),
                        key, value));
                System.out.println(String.format("%s %s %s", LocalTime.now(), key, value));
            }
        }
    }

    private static String createTopic(String topicName, int numberOfPartitions, String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient ac = AdminClient.create(props)) {
            if(!hasTopic(topicName, ac)) {
                NewTopic topic = new NewTopic(topicName, numberOfPartitions, (short) 1);
                CreateTopicsResult result = ac.createTopics(Collections.singletonList(topic));
                result.all().get();
            }
            return topicName;
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean hasTopic(String topicName, AdminClient ac) throws InterruptedException, ExecutionException {
        DescribeTopicsResult topic1 = ac.describeTopics(Collections.singletonList(topicName));
        return topic1.all().get().size() > 0;
    }


    private static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }
}
