package kt.consumer;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Set;

public class ConsumerOptions {
    public Set<String> topics;

    public String broker;

    /**
     * Consume starting with this timestamp for all topics.
     * Cannot be used together with {@link ConsumerOptions#fromBeginning}.
     */
    @Nullable
    public Instant startConsumerLimit;

    /**
     * Stop consuming records when reaching this timestamp.
     */
    @Nullable
    public Instant endConsumerLimit;

    /**
     * Whether to consume all topics from their first offset.
     * Cannot be used together with {@link ConsumerOptions#startConsumerLimit}.
     */
    public boolean fromBeginning;

    /**
     * Whether to output records sorted by their timestamp.
     */
    public boolean sortRecords;


    void validate() {
        if(endConsumerLimit!=null && endConsumerLimit.isAfter(Instant.now())) {
            throw new IllegalStateException("endConsumerLimit must be in the past");
        }
        if(startConsumerLimit!=null && fromBeginning){
            throw new IllegalStateException("startConsumerLimit cannot be used with fromBeginning");
        }
    }

    /**
     * @return true if historical records are to be fetched.
     */
    boolean shouldReadHistoricalRecords() {
        return startConsumerLimit != null || fromBeginning;
    }
}