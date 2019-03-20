package kt.manual;

import kt.markers.InternalState;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.FIVE_SECONDS;

public class ManualTestProducer {

    private AtomicBoolean keepRunning = new AtomicBoolean(true);

    private List<String> topics = new ArrayList<>();
    private String bootstrapServers;
    private int topicNo;
    private int partitionNo;

    @InternalState
    private AtomicLong producedRecordCount = new AtomicLong();

    public static void main(String[] args) {
        double MAX_SECONDS = 0.1;
        int TOPIC_NO = 10;
        int PARTITION_NO = 3;
        ManualTestProducer producer = new ManualTestProducer("localhost:9092", TOPIC_NO, PARTITION_NO, false);
        producer.start(MAX_SECONDS, Duration.ZERO);
    }

    ManualTestProducer(String bootstrapServers, int topicNo, int partitionNo, boolean deleteExistingTopics) {
        this.bootstrapServers = bootstrapServers;
        this.topicNo = topicNo;
        this.partitionNo = partitionNo;
        for (int i = 0; i < topicNo; i++) {
            topics.add("topic" + i);
        }
        if (deleteExistingTopics) {
            System.out.println("Deleting topics " + topics);
            delteTopics(topics, bootstrapServers);
        }
        topics.forEach(topicName -> createTopic(topicName, partitionNo, bootstrapServers));
    }

    void start(double maxSeconds, Duration delayProduce) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        sleep((int) delayProduce.toMillis());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; ; i++) {
                try {
                    sleep((int) ((Math.random() * maxSeconds) * 1000));
                    if (!keepRunning.get()) {
                        break;
                    }
                    String key = "key" + i;
                    String value = "val-" + i;
                    String topic = topics.get((int) (Math.random() * topicNo));
                    int partition = (int) (Math.random() * partitionNo);
                    producer.send(new ProducerRecord<>(
                            topic,
                            partition,
                            key, value));
                    producedRecordCount.incrementAndGet();
                    FileLogger.write("PRODUCED time:%s topic:%s partition: %s value: %s", LocalTime.now(), topic, partition, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void stop() {
        keepRunning.set(false);
    }

    Set<String> getTopics() {
        return new HashSet<>(topics);
    }

    long getProducedRecordCount() {
        return producedRecordCount.get();
    }

    private static void createTopic(String topicName, int numberOfPartitions, String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient ac = AdminClient.create(props)) {
            TopicDescription existingTopic = fetchTopic(topicName, ac);
            boolean createNewTopic = false;
            if (existingTopic == null) {
                createNewTopic = true;
            } else if (existingTopic.partitions().size() != numberOfPartitions) {
                System.out.println(String.format("Requested partitions do not match for topic %s. Deleting existing and creating new one.", topicName));
                deleteTopic(topicName, ac);
                createNewTopic = true;
            }

            if (createNewTopic) {
                await().atMost(FIVE_SECONDS).until(() -> {
                    try {
                        NewTopic topic = new NewTopic(topicName, numberOfPartitions, (short) 1);
                        CreateTopicsResult result = ac.createTopics(Collections.singletonList(topic));
                        result.all().get();
                    } catch (ExecutionException e) {
                        return false;
                    }
                    return true;
                });
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void delteTopics(List<String> topics, String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient ac = AdminClient.create(props)) {
            DeleteTopicsResult result = ac.deleteTopics(topics);
            result.all().get();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            // ignore
        }
    }

    private static void deleteTopic(String topicName, AdminClient ac) throws InterruptedException, ExecutionException {
        DeleteTopicsResult result = ac.deleteTopics(Collections.singletonList(topicName));
        result.all().get();
    }

    @Nullable
    private static TopicDescription fetchTopic(String topicName, AdminClient ac) throws InterruptedException {
        DescribeTopicsResult topic1 = ac.describeTopics(Collections.singletonList(topicName));
        try {

            Map<String, TopicDescription> found = topic1.all().get();
            if (found.size() != 1) {
                return null;
            }
            return found.get(topicName);
        } catch (ExecutionException e) {
            return null;
        }
    }


    private static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }
}
