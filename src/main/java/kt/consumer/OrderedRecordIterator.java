package kt.consumer;

import kt.markers.InternalState;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Iterator that traverses records in order of their timestamp.
 * Given multiple partitions, each of them holding an ordered set of records,
 * this iterator establishes an order over the whole set of records.
 * The order is given by the timestamp of each record. Note that a missing timestamp
 * is notified by -1. Since missing timestamps provide no ordering information they
 * have to be kept together with their neighbours.
 *
 * Example:
 * p1: 2, 6, 9
 * p2: 2, 5, 7
 * Output: (p1, 2),(p2, 2),(p2, 5),(p1, 6),(p2, 7),(p1, 9)
 *
 * Example:
 * p1: 2, -1, -1, 8, 9
 * p2: 2, 5, -1, 6, -1, 10
 * Output: (p1, 2),(p1, -1),(p1, -1),(p2, 2),(p2, 5),(p2, -1),(p2, 6),(p2, -1),(p1, 8),(p1, 9),(p2, 10)
 */
class OrderedRecordIterator implements Iterator<ConsumerRecord<String, String>> {
    private static class Partition {
        List<ConsumerRecord<String, String>> records;
        int index = 0;
        boolean reachedEnd;

        Partition(List<ConsumerRecord<String, String>> records) {
            this.records = records;
            reachedEnd = records.isEmpty();
        }

        void moveToNextRecord() {
            index++;
            if(index>=records.size()){
                reachedEnd = true;
            }
        }
        ConsumerRecord<String, String> currentRecord() {
            return records.get(index);
        }
    }

    private List<Partition> partitions;

    @Nullable @InternalState
    private ConsumerRecord<String, String> next;

    /**
     * @param partitions Each partition holds an ordered set of records.
     */
    OrderedRecordIterator(Collection<List<ConsumerRecord<String, String>>> partitions) {
        this.partitions = partitions.stream()
                .map(Partition::new).collect(Collectors.toList());
    }

    @Override
    public boolean hasNext() {
        boolean reachedEnd = true;
        for (Partition partition : partitions) {
            if(!partition.reachedEnd && partition.records.get(partition.index).timestamp()==-1) {
                next = partition.records.get(partition.index);
                partition.moveToNextRecord();
                return true;
            }
            if (!partition.reachedEnd) {
                reachedEnd = false;
            }
        }
        if (reachedEnd) {
            return false;
        }

        Partition nextMin = findNextMin();
        next = nextMin.currentRecord();
        nextMin.moveToNextRecord();

        return true;
    }

    @Override
    public ConsumerRecord<String, String> next() {
        return next;
    }

    private Partition findNextMin() {
        return partitions.stream()
                .filter(partition -> !partition.reachedEnd)
                .min((pa, pb) -> (int) (pa.records.get(pa.index).timestamp() - pb.records.get(pb.index).timestamp()))
                .orElseThrow(() -> new IllegalStateException("Could not find partition minimum"));
    }

}
