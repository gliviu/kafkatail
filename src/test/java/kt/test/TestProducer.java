package kt.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.awaitility.Duration;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestProducer {
    private static KafkaProducer<String, String> producer;
    private static final ExecutorService executor = Executors.newFixedThreadPool(20);

    /**
     * Run once per test session.
     */
    public static void init(String bootstrapServer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    /**
     * Run once per test session.
     */
    public static void close() {
        producer.close();
    }


    public static void produce(String topic, String key, String val, int partition) {
        Future<RecordMetadata> res = producer.send(new ProducerRecord<>(topic, partition, key, val));
        try {
            RecordMetadata record = res.get(1, TimeUnit.SECONDS);
            System.out.println(String.format("PRODUCED %s %s %s", Instant.ofEpochMilli(record.timestamp()), record.topic(), val));
        } catch (TimeoutException | ExecutionException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void produce(String topic, String key, String val) {
        produce(topic, key, val, 0);
    }

    public static void produce(String topic, String val) {
        produce(topic, null, val);
    }

    public static void produce(String topic, String val, int partition) {
        produce(topic, null, val, partition);
    }

    public static class Stoppable {
        private AtomicBoolean shouldStop = new AtomicBoolean();

        public void stop() {
            shouldStop.set(true);
        }
    }

    /**
     * Produce stream of messages at given rate.
     *
     * @param topic topic
     * @param rate span between each message
     */
    public static Stoppable produceRate(String topic, Duration rate) {
        Stoppable stoppable = new Stoppable();
        CompletableFuture.runAsync(() -> {
            for (int i = 0; ; i++) {
                if(stoppable.shouldStop.get()){
                    break;
                }
                produce(topic, "val" + i);
                TestUtils.sleep(rate);
            }
        }, executor);
        return stoppable;
    }
}
