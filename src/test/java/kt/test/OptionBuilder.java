package kt.test;

import kt.consumer.ConsumerOptions;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static kt.test.TestUtils.bootstrapServers;

public class OptionBuilder {
    private ConsumerOptions options;

    private OptionBuilder() {
        options = new ConsumerOptions();
        options.topics = new HashSet<>();
    }

    public static OptionBuilder options() {
        return new OptionBuilder()
                .broker(bootstrapServers());
    }

    public ConsumerOptions build() {
        if (options.topics == null || options.broker == null) {
            throw new IllegalArgumentException("Incomplete options");
        }
        return options;
    }

    public OptionBuilder broker(String val) {
        options.broker = val;
        return this;
    }

    public OptionBuilder topics(String ...val) {
        options.topics.addAll(Arrays.asList(val));
        return this;
    }

    public OptionBuilder topics(List<String> val) {
        options.topics.addAll(val);
        return this;
    }

    public OptionBuilder startConsumerLimit(Instant val) {
        options.startConsumerLimit = val;
        return this;
    }

    public OptionBuilder endConsumerLimit(Instant val) {
        options.endConsumerLimit = val;
        return this;
    }

    public OptionBuilder sortRecords(boolean val) {
        options.sortRecords = val;
        return this;
    }


}
