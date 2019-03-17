package kt.consumer;

import kt.markers.InternalState;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

/**
 * Consumes records with no order guarantee and outputs them ordered
 * by their timestamp.
 */
public class InOrderBatchedConsumer {
    static class Limits {
        int maxRecordCount;
        int maxRecordTotalSize;
        Duration maxTimeSinceBatchStart;
        Duration maxTimeSinceLastRecord;

        /**
         * @param maxRecordCount                Maximum number of messages to accumulate before forcing printing sorted records.
         * @param maxRecordTotalSize            Maximum size (in characters) to accumulate before forcing printing sorted records.
         * @param maxTimeSinceLastRecord        How much time to wait between receiving the latest record and starting printing
         *                                      sorted records.
         * @param maxTimeSinceBatchStart For how long to accumulate records.
         */
        public Limits(Duration maxTimeSinceLastRecord, Duration maxTimeSinceBatchStart,
                      int maxRecordCount, int maxRecordTotalSize) {
            this.maxRecordCount = maxRecordCount;
            this.maxRecordTotalSize = maxRecordTotalSize;
            this.maxTimeSinceBatchStart = maxTimeSinceBatchStart;
            this.maxTimeSinceLastRecord = maxTimeSinceLastRecord;
        }

        Limits() {}
    }

    private static class Stats {
        /**
         * Time when the last record is received.
         */
        Instant lastRecordTime = Instant.now();

        /**
         * Time when last {@link InOrderBatchedConsumer#process()} occurred.
         */
        Instant lastProcesstime = Instant.now();

        /**
         * Total count of records.
         */
        long recordCount = 0;

        /**
         * Total size (in characters) of all records.
         */
        long recordsTotalSize = 0;
    }

    @InternalState
    private Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsByPartition = new HashMap<>();
    @InternalState
    private Stats stats = new Stats();


    private Consumer<ConsumerRecord<String, String>> recordConsumer;
    private Limits limits;

    InOrderBatchedConsumer(Limits limits, Consumer<ConsumerRecord<String, String>> recordConsumer) {
        this.limits = limits;
        this.recordConsumer = recordConsumer;
    }

    /**
     * Call this when new records are provided by {@link KafkaConsumer#poll(Duration)}.
     *
     * @param record list of new record
     */
    void acceptRecord(ConsumerRecord<String, String> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        recordsByPartition.computeIfAbsent(partition, k -> new ArrayList<>()).add(record);
        stats.recordCount++;
        stats.recordsTotalSize += record.value().length();
        stats.lastRecordTime = Instant.now();
//        System.out.println("5667 lr "+stats.lastRecordTime);
    }

    /**
     * Call this periodically to allow producer output ordered records.
     */
    void process() {
//        System.out.println("5667 a0 "+maxTimeSinceLastRecord.getSeconds());
//        System.out.println("5667 a01 "+maxTimeSinceBatchStart.getSeconds());
//        System.out.println("5667 a1 "+recordsByPartition.size());
        if (recordsByPartition.isEmpty()) {
            return;
        }
        boolean exceedsSize = stats.recordsTotalSize > limits.maxRecordTotalSize;
        boolean exceedsCount = stats.recordCount > limits.maxRecordCount;
        boolean exceedsDelay = Duration.between(stats.lastRecordTime, Instant.now()).compareTo(limits.maxTimeSinceLastRecord) > 0;
        boolean exceedsRecordAccumulationInterval = Duration.between(stats.lastProcesstime, Instant.now()).compareTo(limits.maxTimeSinceBatchStart) > 0;
        if (exceedsCount || exceedsDelay || exceedsSize || exceedsRecordAccumulationInterval) {
//            System.out.println(Instant.now()+" 5667 a2 lr "+Duration.between(stats.lastRecordTime, Instant.now()).getSeconds());
//            System.out.println(Instant.now()+" 5667 a2 mai "+Duration.between(stats.lastProcesstime, Instant.now()).getSeconds());
            stats.lastProcesstime = Instant.now();
            Iterable<ConsumerRecord<String, String>> recordSorter = () -> new OrderedRecordIterator(recordsByPartition.values());
            StreamSupport.stream(recordSorter.spliterator(), false).forEach(recordConsumer::accept);

            // reset state
            recordsByPartition = new HashMap<>();
            stats = new Stats();
        }
    }

    /**
     * @param maxTimeSinceLastRecord see {@link Limits#Limits(Duration, Duration, int, int)}
     */
    void updateMaxTimeSinceLastRecord(Duration maxTimeSinceLastRecord) {
        this.limits.maxTimeSinceLastRecord = maxTimeSinceLastRecord;
    }

    /**
     * @param maxTimeSinceBatchStart see {@link Limits#Limits(Duration, Duration, int, int)}
     */
    void updateMaxTimeSinceBatchStart(Duration maxTimeSinceBatchStart) {
        this.limits.maxTimeSinceBatchStart = maxTimeSinceBatchStart;
    }

}
