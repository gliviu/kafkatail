package kt.consumer;

/**
 * A partition of timestamps. Timestaps are supposed to be ordered.
 */
class TimestampPartition {
    int partitionId;
    String topicName;
    long[] timestamps;

    private TimestampPartition(int name, String topicName, long[] timestamps) {
        this.partitionId = name;
        this.topicName = topicName;
        this.timestamps = timestamps;
    }

    static TimestampPartition partition(String partitionName, long... items) {
        return partition(partitionName, partitionName, items);
    }

    static TimestampPartition partition(String topicName, String partitionName, long... items) {
        int partitionId = Integer.parseInt(partitionName.substring(1));
        return new TimestampPartition(partitionId, topicName, items);
    }

}