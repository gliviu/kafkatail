package kt.consumer;

import kt.consumer.InOrderBatchedConsumer.Limits;
import kt.markers.InternalState;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static kt.consumer.ConsumerEvent.POLL_RECORDS;

public class MultiConsumer {
    private final Duration OFFSET_SEEK_TIMEOUT = Duration.ofSeconds(30);
    private final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    private final Duration ALL_TOPIC_INFO_TIMEOUT = Duration.ofSeconds(30);
    private final Duration ORDERED_RECORD_PRODUCER_DELAY_NEW_RECORDS = POLL_TIMEOUT;
    private final Duration ORDERED_RECORD_PRODUCER_DELAY_HISTORICAL_RECORDS = PartitionEndLimitState.END_LIMIT_OFFSET_THRESOLD;
    private final Duration ORDERED_RECORD_PRODUCER_MAX_ACCUMULATION_INTERVAL_NEW_RECORDS = Duration.ofSeconds(2);
    private final Duration ORDERED_RECORD_PRODUCER_MAX_ACCUMULATION_INTERVAL_HISTORICAL_RECORDS = Duration.ofSeconds(20);
    private final int ORDERED_RECORD_PRODUCER_MAX_RECORD_COUNT = 100000;
    private final int ORDERED_RECORD_PRODUCER_MAX_RECORD_TOTAL_SIZE = ORDERED_RECORD_PRODUCER_MAX_RECORD_COUNT * 200;

    @InternalState
    private AtomicBoolean keepRunning = new AtomicBoolean(false);

    private InOrderBatchedConsumer inOrderBatchedConsumer;

    private ConsumerContext context = new ConsumerContext();

    public void start(ConsumerOptions options, Consumer<ConsumerEvent> eventConsumer,
                      Consumer<ConsumerRecord<String, String>> recordConsumer) {
        if (keepRunning.get()) {
            throw new IllegalStateException("Consumer already started");
        }
        keepRunning.set(true);
        context.options = options;
        context.eventConsumer = eventConsumer;
        context.recordConsumer = recordConsumer;
        options.validate();
        Properties props = configureKafkaConsumer(options);
        if (options.shouldReadHistoricalRecords()) {
            inOrderBatchedConsumer = new InOrderBatchedConsumer(inOrderBatchedConsumerLimitsForHistoricalRecords(), context);
        } else {
            inOrderBatchedConsumer = new InOrderBatchedConsumer(inOrderBatchedConsumerLimitsForNewRecords(), context);
        }
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            eventConsumer.accept(ConsumerEvent.GET_PARTITIONS);
            List<TopicPartition> partitions = getPartitions(options.broker, options.topics);
            eventConsumer.accept(ConsumerEvent.ASSIGN_PARTITIONS);
            consumer.assign(partitions);
            if (options.startConsumerLimit != null) {
                eventConsumer.accept(ConsumerEvent.SEEK_BACK);
                seekBack(consumer, partitions, options.startConsumerLimit);
            } else if (options.fromBeginning) {
                eventConsumer.accept(ConsumerEvent.SEEK_TO_BEGINNING);
                consumer.seekToBeginning(partitions);
            } else {
                // Reset offsets. We want to receive only messages received after current time even if we reuse consumer.
                eventConsumer.accept(ConsumerEvent.SEEK_TO_END);
                consumer.seekToEnd(partitions);
            }
            eventConsumer.accept(ConsumerEvent.START_CONSUME);

            PartitionEndLimitState partitionEndLimitState = new PartitionEndLimitState(
                    consumer.endOffsets(partitions, OFFSET_SEEK_TIMEOUT),
                    options, eventConsumer);
            while (keepRunning.get()) {
                eventConsumer.accept(POLL_RECORDS);
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                inOrderBatchedConsumer.process();
                if (options.shouldReadHistoricalRecords()) {
                    partitionEndLimitState.acceptRecords(records);
                    if (partitionEndLimitState.hasReachedEndLimit()) {
                        inOrderBatchedConsumer.updateMaxTimeSinceLastRecord(
                                ORDERED_RECORD_PRODUCER_DELAY_NEW_RECORDS);
                        inOrderBatchedConsumer.updateMaxTimeSinceBatchStart(
                                ORDERED_RECORD_PRODUCER_MAX_ACCUMULATION_INTERVAL_NEW_RECORDS);
                    }
                }
                if (options.endConsumerLimit != null) {
                    consumeRecordsWithinLimits(records, options.endConsumerLimit, recordConsumer, partitionEndLimitState);
                } else if (options.startConsumerLimit != null) {
                    consumeRecordsFromStartLimit(records, recordConsumer);
                } else {
                    consumeNewRecords(records, recordConsumer);
                }
            }
            eventConsumer.accept(ConsumerEvent.END_CONSUME);
        }
    }

    /**
     * Consume new records.
     */
    private void consumeNewRecords(
            ConsumerRecords<String, String> records, Consumer<ConsumerRecord<String, String>> recordConsumer) {
        for (ConsumerRecord<String, String> record : records) {
            if (context.options.sortRecords) {
                inOrderBatchedConsumer.acceptRecord(record);
            } else {
                recordConsumer.accept(record);
            }
        }
    }

    /**
     * Consume historical records starting from limit.
     * Historical records may come out of order. We'll have to sort them.
     */
    private void consumeRecordsFromStartLimit(
            ConsumerRecords<String, String> records, Consumer<ConsumerRecord<String, String>> recordConsumer) {
        StreamSupport
                .stream(records.spliterator(), false)
                .sorted(Comparator.comparing(ConsumerRecord::timestamp))
                .forEach(record -> {
                    if (context.options.sortRecords) {
                        inOrderBatchedConsumer.acceptRecord(record);
                    } else {
                        recordConsumer.accept(record);
                    }
                });
    }

    private static class Counter {
        long val = 0;
    }

    /**
     * Consume historical records between limits.
     * Historical records may come out of order. We'll have to sort them.
     * Also stop consuming if end limit is reached.
     */
    private void consumeRecordsWithinLimits(
            ConsumerRecords<String, String> records, Instant endConsumerLimit,
            Consumer<ConsumerRecord<String, String>> recordConsumer, PartitionEndLimitState partitionEndLimitState) {
        Counter recordsBeforeEndLimit = new Counter();
        StreamSupport.stream(records.spliterator(), false)
                .sorted(Comparator.comparing(ConsumerRecord::timestamp))
                .filter(record -> record.timestamp() <= endConsumerLimit.toEpochMilli())
                .forEach(record -> {
                    if (context.options.sortRecords) {
                        inOrderBatchedConsumer.acceptRecord(record);
                    } else {
                        recordConsumer.accept(record);
                    }
                    recordsBeforeEndLimit.val++;
                });
        if (partitionEndLimitState.hasReachedEndLimit()) {
            stop();
        }
    }

    /**
     * Intended for debugging.
     */
    @SuppressWarnings("unused")
    private String debugStr(ConsumerRecord<String, String> record) {
        return String.format("timestamp:%s, topic:%s, key:%s, value:%s",
                Instant.ofEpochMilli(record.timestamp()), record.topic(), record.key(), record.value());
    }

    /**
     * Intended for debugging.
     */
    @SuppressWarnings("unused")
    private String localDateTimeDebugStr(Instant instant) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        return localDateTime.format(dateTimeFormatter);
    }

    public void stop() {
        keepRunning.set(false);
    }

    private Properties configureKafkaConsumer(ConsumerOptions options) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.broker);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // don't need any offset persisted
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private class PartitionOffset {
        public TopicPartition partition;
        @Nullable
        Long offset;

        PartitionOffset(TopicPartition partition, @Nullable Long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }

    private void seekBack(KafkaConsumer<String, String> consumer,
                          List<TopicPartition> partitions, Instant startConsumerLimit) {
        Map<TopicPartition, Long> timestampsToSearch = partitions.stream().collect(Collectors.toMap(
                topicPartition -> topicPartition, topicPartition -> startConsumerLimit.toEpochMilli()));
        Map<TopicPartition, OffsetAndTimestamp> partitionsToOffsets =
                consumer.offsetsForTimes(timestampsToSearch, OFFSET_SEEK_TIMEOUT);
        partitionsToOffsets
                .entrySet().stream()
                .map(entry -> new PartitionOffset(entry.getKey(), entry.getValue() == null ? null : entry.getValue().offset()))
                .filter(partitionOffset -> partitionOffset.offset != null)
                .forEach(partitionOffset -> consumer.seek(partitionOffset.partition, partitionOffset.offset));
    }

    private List<TopicPartition> getPartitions(String bootstrapServers, Set<String> topics) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        try (AdminClient ac = AdminClient.create(props)) {
            DescribeTopicsResult res = ac.describeTopics(topics, new DescribeTopicsOptions().timeoutMs((int) ALL_TOPIC_INFO_TIMEOUT.toMillis()));
            Map<String, TopicDescription> topicDescriptions = res.all().get();
            return topicDescriptions.values().stream()
                    .flatMap(topicDescription -> {
                        String topic = topicDescription.name();
                        return topicDescription
                                .partitions().stream()
                                .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()));
                    })
                    .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private Limits inOrderBatchedConsumerLimitsForHistoricalRecords() {
        return new Limits(
                ORDERED_RECORD_PRODUCER_DELAY_HISTORICAL_RECORDS,
                ORDERED_RECORD_PRODUCER_MAX_ACCUMULATION_INTERVAL_HISTORICAL_RECORDS,
                ORDERED_RECORD_PRODUCER_MAX_RECORD_COUNT, ORDERED_RECORD_PRODUCER_MAX_RECORD_TOTAL_SIZE);
    }

    private Limits inOrderBatchedConsumerLimitsForNewRecords() {
        return new Limits(
                ORDERED_RECORD_PRODUCER_DELAY_NEW_RECORDS,
                ORDERED_RECORD_PRODUCER_MAX_ACCUMULATION_INTERVAL_NEW_RECORDS,
                ORDERED_RECORD_PRODUCER_MAX_RECORD_COUNT, ORDERED_RECORD_PRODUCER_MAX_RECORD_TOTAL_SIZE);
    }

}
