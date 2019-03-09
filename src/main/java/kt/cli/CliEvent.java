package kt.cli;

import kt.consumer.ConsumerEvent;

public enum CliEvent {
    WARNING_OCCURRED,
    GET_ALL_TOPICS,
    GET_ALL_TOPICS_END,
    GET_PARTITIONS,
    ASSIGN_PARTITIONS,
    /**
     * {@link ConsumerEvent#SEEK_BACK}
     */
    SEEK_BACK,
    /**
     * {@link ConsumerEvent#SEEK_TO_END}
     */
    SEEK_TO_END,
    /**
     * {@link ConsumerEvent#SEEK_TO_BEGINNING}
     */
    SEEK_TO_BEGINNING,
    /**
     * {@link ConsumerEvent#START_CONSUME}
     */
    START_CONSUME,
    /**
     * {@link ConsumerEvent#REACHED_END_CONSUMER_LIMIT}
     */
    REACHED_END_CONSUMER_LIMIT,
    /**
     * {@link ConsumerEvent#CONSUMING_NEW_RECORDS}
     */
    CONSUMING_NEW_RECORDS,
    /**
     * {@link ConsumerEvent#END_CONSUME}
     */
    END_CONSUME,
    ;
}
