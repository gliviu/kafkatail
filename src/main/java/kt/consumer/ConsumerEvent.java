package kt.consumer;

public enum ConsumerEvent {
    GET_PARTITIONS,
    ASSIGN_PARTITIONS,
    /**
     * Seek consumer position back for all topics.
     */
    SEEK_BACK,
    /**
     * Seek consumer position to the end of the stream for all topics.
     */
    SEEK_TO_END,
    /**
     * Seek consumer position to the front of the stream for all topics.
     */
    SEEK_TO_BEGINNING,
    /**
     * All initialization done. Start consuming.
     */
    START_CONSUME,
    /**
     * Consumer end limit reached. Stop consumer.
     */
    REACHED_END_CONSUMER_LIMIT,
    /**
     * Consumed all historical records. Reading only new records from now on.
     */
    CONSUMING_NEW_RECORDS,
    /**
     * Polling for new records.
     */
    POLL_RECORDS,
    /**
     * Triggered when {@link InOrderBatchedConsumer} starts a new batch of ordered records.
     * Note that it is possible that multiple start events to be
     * triggered without a corresponding {@link ConsumerEvent#END_ORDERING_RECORDS}.
     */
    START_ORDERING_RECORDS,
    /**
     * Triggered when {@link InOrderBatchedConsumer} finished a batch of ordered records
     * that are ready to be printed.
     */
    END_ORDERING_RECORDS,
    /**
     * Consumer stopped.
     */
    END_CONSUME
}
