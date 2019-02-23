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
     * Consumer stopped.
     */
    END_CONSUME
}
