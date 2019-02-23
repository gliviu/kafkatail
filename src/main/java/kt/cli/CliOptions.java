package kt.cli;

import javax.annotation.Nullable;
import java.util.Set;

public class CliOptions {
    public Set<String> excludeTopics;

    public Set<String> topicPatterns;

    @Nullable
    public String broker;

    @Nullable
    public String startConsumerLimit;

    @Nullable
    public String consumeDuration;

    @Nullable
    public String endConsumerLimit;

    @Nullable
    public String lines;

    public boolean help;

    public boolean version;

    public boolean shouldConsumeAllTopics() {
        return topicPatterns.isEmpty();
    }

}
