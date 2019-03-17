package kt.consumer;

import kt.consumer.InOrderBatchedConsumer.Limits;
import kt.test.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static kt.consumer.TimestampPartition.partition;
import static org.assertj.core.api.Assertions.assertThat;

/**
 */
@DisplayName("InOrderBatchedConsumer")
class TestInOrderBatchedConsumer {
    static class RecordsConsumerMock implements Consumer<ConsumerRecord<String, String>> {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();

        @Override
        public void accept(ConsumerRecord<String, String> record) {
            records.add(record);
        }
    }

    static class Timer {
        private Instant start = Instant.now();

        boolean reached(Duration timeout) {
            return Duration.between(start, Instant.now()).compareTo(timeout) > 0;
        }
//        Timer timer = new Timer();
//        while(!timer.reached(Duration.ofMillis(800))) {
//            TestUtils.sleep(Duration.ofMillis(500));
//            createRecords(partition("p1", 3, 9, -1, -1)).forEach(orderedConsumer::acceptRecord);
//            orderedConsumer.acceptRecord();
//        }
    }

    @Test
    @DisplayName("outputs data from one partition")
    void t3754() {
        RecordsConsumerMock consumer = new RecordsConsumerMock();
        Limits limits = new Limits();
        limits.maxTimeSinceLastRecord =Duration.ofSeconds(1);
        limits.maxTimeSinceBatchStart = Duration.ofSeconds(10);
        limits.maxRecordTotalSize = 1000;
        limits.maxRecordCount = 100;
        InOrderBatchedConsumer orderedConsumer = new InOrderBatchedConsumer(limits, consumer);

        createRecords(partition("t1", "p1", 2, 4, 5, 7)).forEach(orderedConsumer::acceptRecord);
        TestUtils.sleep(Duration.ofMillis(100));
        orderedConsumer.process();
        assertThat(consumer.records).isEmpty();
        TestUtils.sleep(Duration.ofMillis(1000));
        orderedConsumer.process();
        assertThat(consumer.records).isNotEmpty();
        assertThat(records(consumer.records)).isEqualTo("(t1p1, 2),(t1p1, 4),(t1p1, 5),(t1p1, 7)");
    }


    @Test
    @DisplayName("outputs sorted data from one topic, multiple partition")
    void t2468() {
        RecordsConsumerMock consumer = new RecordsConsumerMock();
        Limits limits = new Limits();
        limits.maxTimeSinceLastRecord =Duration.ofSeconds(1);
        limits.maxTimeSinceBatchStart = Duration.ofSeconds(10);
        limits.maxRecordTotalSize = 1000;
        limits.maxRecordCount = 100;
        InOrderBatchedConsumer orderedConsumer = new InOrderBatchedConsumer(limits, consumer);

        createRecords(
                partition("t1", "p1", 2, 4, 5, 7),
                partition("t1", "p2", 1, 5, 8)
        ).forEach(orderedConsumer::acceptRecord);
        TestUtils.sleep(Duration.ofMillis(100));
        orderedConsumer.process();
        assertThat(consumer.records).isEmpty();
        TestUtils.sleep(Duration.ofMillis(1000));
        orderedConsumer.process();
        assertThat(consumer.records).isNotEmpty();
        assertThat(records(consumer.records)).isEqualTo("(t1p2, 1),(t1p1, 2),(t1p1, 4),(t1p2, 5),(t1p1, 5),(t1p1, 7),(t1p2, 8)");

    }

    @Test
    @DisplayName("outputs sorted data from multiple topics, multiple partitions")
    void t4267() {
        RecordsConsumerMock consumer = new RecordsConsumerMock();
        Limits limits = new Limits();
        limits.maxTimeSinceLastRecord =Duration.ofSeconds(1);
        limits.maxTimeSinceBatchStart = Duration.ofSeconds(10);
        limits.maxRecordTotalSize = 1000;
        limits.maxRecordCount = 100;
        InOrderBatchedConsumer orderedConsumer = new InOrderBatchedConsumer(limits, consumer);

        createRecords(
                partition("t1", "p1", 2, 5, 7),
                partition("t2", "p1", 3, 4, 7),
                partition("t1", "p2", 1, 5, 8)
        ).forEach(orderedConsumer::acceptRecord);
        TestUtils.sleep(Duration.ofMillis(100));
        orderedConsumer.process();
        assertThat(consumer.records).isEmpty();
        TestUtils.sleep(Duration.ofMillis(1000));
        orderedConsumer.process();
        assertThat(consumer.records).isNotEmpty();
        assertThat(records(consumer.records)).isEqualTo("(t1p2, 1),(t1p1, 2),(t2p1, 3),(t2p1, 4),(t1p2, 5),(t1p1, 5),(t1p1, 7),(t2p1, 7),(t1p2, 8)");

    }

    @Test
    @DisplayName("accumulates multiple data series before sorting")
    void t3495() {
        RecordsConsumerMock consumer = new RecordsConsumerMock();
        Limits limits = new Limits();
        limits.maxTimeSinceLastRecord =Duration.ofSeconds(1);
        limits.maxTimeSinceBatchStart = Duration.ofSeconds(10);
        limits.maxRecordTotalSize = 1000;
        limits.maxRecordCount = 100;
        InOrderBatchedConsumer orderedConsumer = new InOrderBatchedConsumer(limits, consumer);

        createRecords(
                partition("t1", "p1", 2, 7),
                partition("t1", "p2", 1, 4)
        ).forEach(orderedConsumer::acceptRecord);
        TestUtils.sleep(Duration.ofMillis(100));
        createRecords(
                partition("t1", "p1", 8, 11),
                partition("t1", "p2", 6, 7)
        ).forEach(orderedConsumer::acceptRecord);
        TestUtils.sleep(Duration.ofMillis(100));
        orderedConsumer.process();
        assertThat(consumer.records).isEmpty();
        TestUtils.sleep(Duration.ofMillis(1000));
        orderedConsumer.process();
        assertThat(consumer.records).isNotEmpty();
        assertThat(records(consumer.records)).isEqualTo("(t1p2, 1),(t1p1, 2),(t1p2, 4),(t1p2, 6),(t1p2, 7),(t1p1, 7),(t1p1, 8),(t1p1, 11)");

    }

    @Test
    @DisplayName("does not sort if max interval is reached")
    void t3548() {
        RecordsConsumerMock consumer = new RecordsConsumerMock();
        Limits limits = new Limits();
        limits.maxTimeSinceLastRecord =Duration.ofMillis(200);
        limits.maxTimeSinceBatchStart = Duration.ofSeconds(10);
        limits.maxRecordTotalSize = 1000;
        limits.maxRecordCount = 100;
        InOrderBatchedConsumer orderedConsumer = new InOrderBatchedConsumer(limits, consumer);

        createRecords(
                partition("t1", "p1", 2, 7),
                partition("t1", "p2", 1, 4)
        ).forEach(orderedConsumer::acceptRecord);
        orderedConsumer.process();
        assertThat(consumer.records).isEmpty();

        TestUtils.sleep(Duration.ofMillis(300));
        orderedConsumer.process();
        assertThat(consumer.records).hasSize(4);
        createRecords(
                partition("t1", "p1", 8, 11),
                partition("t1", "p2", 6, 9)
        ).forEach(orderedConsumer::acceptRecord);
        orderedConsumer.process();
        assertThat(consumer.records).hasSize(4);

        TestUtils.sleep(Duration.ofMillis(300));
        orderedConsumer.process();
        assertThat(consumer.records).hasSize(8);

        assertThat(records(consumer.records)).isEqualTo("(t1p2, 1),(t1p1, 2),(t1p2, 4),(t1p1, 7),(t1p2, 6),(t1p1, 8),(t1p2, 9),(t1p1, 11)");

    }


    private String records(List<ConsumerRecord<String, String>> records) {
        return records.stream()
                .map(ConsumerRecord::value)
                .collect(Collectors.joining(","));
    }


    private List<ConsumerRecord<String, String>> createRecords(TimestampPartition... partitions) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (TimestampPartition partition : partitions) {
            for (long timestamp : partition.timestamps) {
                String value = String.format("(%sp%s, %s)",
                        partition.topicName, partition.partitionId, timestamp == -1 ? "N" : timestamp);
                ConsumerRecord<String, String> record = new ConsumerRecord<>(
                        partition.topicName, partition.partitionId, 0, timestamp,
                        TimestampType.CREATE_TIME, 0, 0, 0, null, value);
                records.add(record);
            }
        }
        return records;
    }

}
