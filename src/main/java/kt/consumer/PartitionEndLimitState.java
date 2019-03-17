package kt.consumer;

import kt.markers.InternalState;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * One partition is said to have reached 'end limit' if it consumed all historical records and now
 * it waiting for new records to arrive.
 */
class PartitionEndLimitState {

    static final Duration END_LIMIT_OFFSET_THRESOLD = Duration.ofSeconds(5);

    private static class PartitionState {
        /**
         * Current offset reached by partition. If this is greater or equal to stored end limit,
         * it means partition consumes only new records.
         */
        long currentOffset = 0;

        /**
         * Time since we last know records for this partition.
         * If we don't receive records for {@link PartitionEndLimitState#END_LIMIT_OFFSET_THRESOLD} the
         * partition is considered to have reacched end limit.
         */
        Instant lastUpdated = Instant.now();

        /**
         * This is the latest known offset of this partition according to
         * {@link org.apache.kafka.clients.consumer.KafkaConsumer#endOffsets(Collection)}.
         */
        long endLimitOffset;

        PartitionState(long endOffset) {
            this.endLimitOffset = endOffset;
        }

        boolean reachedEndLimit() {
            return currentOffset >= endLimitOffset ||
                    ChronoUnit.SECONDS.between(lastUpdated, Instant.now()) > END_LIMIT_OFFSET_THRESOLD.getSeconds();
        }
    }

    private Consumer<ConsumerEvent> eventConsumer;
    private ConsumerOptions options;

    /**
     * Map partition to their consume status - consuming historical records or not.
     */
    @InternalState
    private Map<TopicPartition, PartitionState> partitionEndLimitStates;

    @InternalState
    private boolean reachedEndLimit = false;

    /**
     * @return true if all partitions reached end limit.
     */
    boolean hasReachedEndLimit() {
        return reachedEndLimit;
    }

    PartitionEndLimitState(Map<TopicPartition, Long> endOffsets, ConsumerOptions options, Consumer<ConsumerEvent> eventConsumer) {
        partitionEndLimitStates = endOffsets.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new PartitionState(entry.getValue())));
        this.options = options;
        this.eventConsumer = eventConsumer;
    }

    /**
     * Call this with newly polled records.
     */
    void acceptRecords(ConsumerRecords<String, String> records) {
        if (reachedEndLimit) {
            return;
        }

        Map<TopicPartition, Long> partitionToMaxOffset = StreamSupport.stream(records.spliterator(), false).collect(Collectors.toMap(
                record -> new TopicPartition(record.topic(), record.partition()),
                ConsumerRecord::offset,
                BinaryOperator.maxBy((record1, record2) -> (int) (record1 - record2))));

        for (Map.Entry<TopicPartition, Long> entry : partitionToMaxOffset.entrySet()) {
            TopicPartition partition = entry.getKey();
            Long offset = entry.getValue();
            PartitionState partitionState = partitionEndLimitStates.get(partition);
            partitionState.currentOffset = offset;
            partitionState.lastUpdated = Instant.now();
        }

        long historicalPartitionsCount = partitionEndLimitStates.values().stream()
                .filter(partitionState -> !partitionState.reachedEndLimit())
                .count();
        if (historicalPartitionsCount == 0) {
            reachedEndLimit = true;
            partitionEndLimitStates = null;
            if (options.endConsumerLimit != null) {
                eventConsumer.accept(ConsumerEvent.REACHED_END_CONSUMER_LIMIT);
            } else if (options.startConsumerLimit != null || options.fromBeginning) {
                eventConsumer.accept(ConsumerEvent.CONSUMING_NEW_RECORDS);
            }
        }
    }
}
