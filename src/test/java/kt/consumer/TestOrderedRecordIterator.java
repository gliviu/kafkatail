package kt.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static kt.consumer.TimestampPartition.partition;

@DisplayName("OrderedRecordIterator")
class TestOrderedRecordIterator {


    @DisplayName("orders records by timestamp")
    @Test
    void t2567() {
        Collection<List<ConsumerRecord<String, String>>> topicPartitionListMap = createRecords(
                partition("p1", 2, 6, 9),
                partition("p2", 2, 5, 7));
        OrderedRecordIterator ro = new OrderedRecordIterator(topicPartitionListMap);
        Assertions.assertThat(records(ro)).isEqualTo("(p1, 2),(p2, 2),(p2, 5),(p1, 6),(p2, 7),(p1, 9)");
    }

    @DisplayName("handles one partition")
    @Test
    void t4865() {
        Collection<List<ConsumerRecord<String, String>>> topicPartitionListMap = createRecords(
                partition("p1", -1, 3, 5, -1, -1, 7, 9, -1));
        OrderedRecordIterator ro = new OrderedRecordIterator(topicPartitionListMap);
        Assertions.assertThat(records(ro)).isEqualTo("(p1, N),(p1, 3),(p1, 5),(p1, N),(p1, N),(p1, 7),(p1, 9),(p1, N)");
    }

    @DisplayName("handles one empty partition")
    @Test
    void t4975() {
        Collection<List<ConsumerRecord<String, String>>> topicPartitionListMap = createRecords(
                partition("p1"));
        OrderedRecordIterator ro = new OrderedRecordIterator(topicPartitionListMap);
        Assertions.assertThat(records(ro)).isEqualTo("");
    }

    @DisplayName("handles multiple empty partitions")
    @Test
    void t4687() {
        Collection<List<ConsumerRecord<String, String>>> topicPartitionListMap = createRecords(
                partition("p1"), partition("p2"), partition("p3"));
        OrderedRecordIterator ro = new OrderedRecordIterator(topicPartitionListMap);
        Assertions.assertThat(records(ro)).isEqualTo("");
    }

    @DisplayName("handles multiple empty and non-empty partitions")
    @Test
    void t3591() {
        Collection<List<ConsumerRecord<String, String>>> topicPartitionListMap = createRecords(
                partition("p1", 2, 6, 9),
                partition("p2"),
                partition("p3", 4, 8, 9)
        );
        OrderedRecordIterator ro = new OrderedRecordIterator(topicPartitionListMap);
        Assertions.assertThat(records(ro)).isEqualTo("(p1, 2),(p3, 4),(p1, 6),(p3, 8),(p1, 9),(p3, 9)");
    }

    @DisplayName("orders records with missing timestamp")
    @Test
    void t8914() {
        Collection<List<ConsumerRecord<String, String>>> topicPartitionListMap = createRecords(
                partition("p1", 2, -1, -1, 8, 9),
                partition("p2", 2, 5, -1, 6, -1, 10));
        OrderedRecordIterator ro = new OrderedRecordIterator(topicPartitionListMap);
        Assertions.assertThat(records(ro)).isEqualTo("(p1, 2),(p1, N),(p1, N),(p2, 2),(p2, 5),(p2, N),(p2, 6),(p2, N),(p1, 8),(p1, 9),(p2, 10)");
    }

    @DisplayName("orders records with missing timestamp at beginning")
    @Test
    void t6725() {
        Collection<List<ConsumerRecord<String, String>>> topicPartitionListMap = createRecords(
                partition("p1", -1, -1, 3, 9),
                partition("p2", 2, 5, 10));
        OrderedRecordIterator ro = new OrderedRecordIterator(topicPartitionListMap);
        Assertions.assertThat(records(ro)).isEqualTo("(p1, N),(p1, N),(p2, 2),(p1, 3),(p2, 5),(p1, 9),(p2, 10)");
    }

    @DisplayName("orders records with missing timestamp at beginning in all partitions")
    @Test
    void t5472() {
        Collection<List<ConsumerRecord<String, String>>> topicPartitionListMap = createRecords(
                partition("p1", -1, -1, 3, 9),
                partition("p2", -1, -1, 5, 10));
        OrderedRecordIterator ro = new OrderedRecordIterator(topicPartitionListMap);
        Assertions.assertThat(records(ro)).isEqualTo("(p1, N),(p1, N),(p2, N),(p2, N),(p1, 3),(p2, 5),(p1, 9),(p2, 10)");
    }

    @DisplayName("orders records with missing timestamp at the end in all partitions")
    @Test
    void t1348() {
        Collection<List<ConsumerRecord<String, String>>> topicPartitionListMap = createRecords(
                partition("p1", 3, 9, -1, -1),
                partition("p2", 5, 10, -1, -1)
        );
        OrderedRecordIterator ro = new OrderedRecordIterator(topicPartitionListMap);
        Assertions.assertThat(records(ro)).isEqualTo("(p1, 3),(p2, 5),(p1, 9),(p1, N),(p1, N),(p2, 10),(p2, N),(p2, N)");
    }


    @DisplayName("order multiple partitions")
    @Test
    void t3489() {
        Collection<List<ConsumerRecord<String, String>>> topicPartitionListMap = createRecords(
                partition("p1", -1, 2, 6, -1, 9),
                partition("p2"),
                partition("p3", 4, -1, 8, 9, -1),
                partition("p4", 2, 3, 6)
        );
        OrderedRecordIterator ro = new OrderedRecordIterator(topicPartitionListMap);
        Assertions.assertThat(records(ro)).isEqualTo("(p1, N),(p1, 2),(p4, 2),(p4, 3),(p3, 4),(p3, N),(p1, 6),(p1, N),(p4, 6),(p3, 8),(p1, 9),(p3, 9),(p3, N)");
    }

    @DisplayName("orders records with missing timestamp at the end")
    @Test
    void t8431() {
        Collection<List<ConsumerRecord<String, String>>> topicPartitionListMap = createRecords(
                partition("p1", 3, 9, -1, -1),
                partition("p2", 2, 5, 10));
        OrderedRecordIterator ro = new OrderedRecordIterator(topicPartitionListMap);
        Assertions.assertThat(records(ro)).isEqualTo("(p2, 2),(p1, 3),(p2, 5),(p1, 9),(p1, N),(p1, N),(p2, 10)");
    }


    private String records(OrderedRecordIterator ro) {
        Iterable<ConsumerRecord<String, String>> it = () -> ro;
        return StreamSupport.stream(it.spliterator(), false)
                .map(ConsumerRecord::value)
                .collect(Collectors.joining(","));
    }

    private Collection<List<ConsumerRecord<String, String>>> createRecords(TimestampPartition... partitions) {
        Collection<List<ConsumerRecord<String, String>>> res = new ArrayList<>();
        for (TimestampPartition partition : partitions) {
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            res.add(records);
            for (long timestamp : partition.timestamps) {
                String value = String.format("(p%s, %s)", partition.partitionId, timestamp == -1 ? "N" : timestamp);
                ConsumerRecord<String, String> record = new ConsumerRecord<>("", 0, 0, timestamp, TimestampType.CREATE_TIME, 0, 0, 0, null, value);
                records.add(record);
            }
        }
        return res;
    }
}
