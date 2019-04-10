package kt.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Consumer;

class ConsumerContext {
    ConsumerOptions options;
    Consumer<ConsumerEvent> eventConsumer;
    Consumer<ConsumerRecord<String, String>> recordConsumer;
}
