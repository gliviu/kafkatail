package kt.manual;

import java.time.LocalTime;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducerManualTests {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> p = new KafkaProducer<>(props)) {
            for (int i = 0; ; i++) {
                sleep(1000);
                String key = "key" + i;
                String value = "val" + i;
                p.send(new ProducerRecord<>("test-topic", key, value));
//                p.send(new ProducerRecord<>("test-topic2", null, value));
                System.out.println(String.format("%s %s %s", LocalTime.now(), key, value));
            }
        }
    }

    private static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch(InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }
}
