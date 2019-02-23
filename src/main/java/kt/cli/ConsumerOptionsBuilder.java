package kt.cli;

import kt.consumer.ConsumerOptions;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static kt.cli.ConsumerLimits.*;
import static kt.cli.ConsumerLimits.consumerEndLimit;

public class ConsumerOptionsBuilder {

    private InfoPrinter infoPrinter;
    private KafkaAdmin kafkaAdmin;

    ConsumerOptionsBuilder(InfoPrinter infoPrinter, KafkaAdmin kafkaAdmin) {
        this.infoPrinter = infoPrinter;
        this.kafkaAdmin = kafkaAdmin;
    }

    /**
     * Converts cli options in consumer options
     * @param cliOptions cli options
     * @return consumer options
     */
    public ConsumerOptions buildConsumerOptions(CliOptions cliOptions) {
        ConsumerOptions options = new ConsumerOptions();
        options.broker = getBroker(cliOptions.broker);
        convertConsumerLimits(cliOptions, options);
        options.topics = cliOptions.topicPatterns;
        Set<String> allTopics = getAllTopicsFromBroker(options.broker, options, cliOptions);
        Consumer<String> warningPrinter = warning -> infoPrinter.cliEventReceived(CliEvent.WARNING_OCCURRED, warning, options, cliOptions);
        options.topics = cliOptions.shouldConsumeAllTopics() ?
                allTopics : includeBySubstring(allTopics, cliOptions.topicPatterns, warningPrinter);
        options.topics = excludeTopicsBySubstring(options.topics, cliOptions.excludeTopics);
        return options;
    }

    private String getBroker(String broker) {
        broker = broker == null ? "localhost:9092" : broker;
        broker = broker.matches("[^:]+:\\d+") ? broker : broker + ":9092";
        return broker;
    }

    private void convertConsumerLimits(CliOptions cliOptions, ConsumerOptions options) {
        if(cliOptions.lines!=null && cliOptions.startConsumerLimit!=null){
            throw new OptionValidationException("-n cannot be used together with -s.");
        }
        if (cliOptions.startConsumerLimit == null && cliOptions.endConsumerLimit != null) {
            throw new OptionValidationException("End limit (-u) can only be used together with start limit (-s).");
        }
        if (cliOptions.startConsumerLimit == null && cliOptions.consumeDuration != null) {
            throw new OptionValidationException("Amount (-a) can only be used together with start limit (-s).");
        }
        if (cliOptions.startConsumerLimit != null) {
            LocalDateTime startConsumerLimit = consumerStartLimit(cliOptions.startConsumerLimit);
            options.startConsumerLimit = instant(startConsumerLimit);

            if (cliOptions.consumeDuration != null && cliOptions.endConsumerLimit != null) {
                throw new OptionValidationException("End limit (-u) and amount (-a) cannot be used together.");
            } else if (cliOptions.consumeDuration != null) {
                options.endConsumerLimit = instant(consumerEndLimit(startConsumerLimit, amountOfTime(cliOptions.consumeDuration)));
            } else if (cliOptions.endConsumerLimit != null) {
                options.endConsumerLimit = instant(consumerEndLimit(cliOptions.endConsumerLimit));
                if (options.startConsumerLimit.isAfter(options.endConsumerLimit)) {
                    throw new OptionValidationException(String.format("Start limit (-s %s) must be before end limit (-u %s)",
                            LocalDateTime.ofInstant(options.startConsumerLimit, ZoneId.systemDefault()),
                            LocalDateTime.ofInstant(options.endConsumerLimit, ZoneId.systemDefault())));
                }
            }
        }
        if(cliOptions.lines!=null) {
            options.fromBeginning = true;
            if (cliOptions.endConsumerLimit != null) {
                options.endConsumerLimit = instant(consumerEndLimit(cliOptions.endConsumerLimit));
            }
        }

        if (options.endConsumerLimit != null) {
            // add one second so interval is inclusive
            options.endConsumerLimit = options.endConsumerLimit.plus(ofSeconds(1));
        }
    }

    @Nonnull
    private Instant instant(LocalDateTime localDateTime) {
        return localDateTime.toInstant(OffsetDateTime.now().getOffset());
    }

    private Set<String> getAllTopicsFromBroker(String broker, ConsumerOptions options, CliOptions cliOptions) {
        infoPrinter.cliEventReceived(CliEvent.GET_ALL_TOPICS, options, cliOptions);
        Set<String> topics = kafkaAdmin.allTopics(broker);
        infoPrinter.cliEventReceived(CliEvent.GET_ALL_TOPICS_END, options, cliOptions);
        if (topics.isEmpty()) {
            throw new OptionValidationException(String.format("Broker %s has no topics.", broker));
        }
        return topics;

    }

    /**
     * @param allTopics         all topics available in broker
     * @param substringPatterns list of topic substrings to be excluded.
     * @return remaining topics after exclusion patterns have been applied
     */
    private Set<String> excludeTopicsBySubstring(Set<String> allTopics, Set<String> substringPatterns) {
        Set<String> topicsToBeExcluded = substringPatterns.stream()
                .flatMap(substring -> matchBySubstring(allTopics, substring))
                .collect(Collectors.toSet());
        Set<String> remainingTopics = diff(allTopics, topicsToBeExcluded);
        if (remainingTopics.isEmpty()) {
            throw new OptionValidationException("No topics remain after applying exclusion patterns: " + String.join(",", substringPatterns));
        }
        return remainingTopics;
    }

    static class MissingTopicsException extends IllegalStateException {
        MissingTopicsException(String message) {
            super(message);
        }
    }

    /**
     * @param allTopics         all topics available in broker
     * @param substringPatterns list of topic substrings to be included
     * @return topics that match the pattern
     */
    private Set<String> includeBySubstring(Set<String> allTopics, Set<String> substringPatterns, Consumer<String> warningConsumer) {
        Set<String> topics = substringPatterns.stream()
                .flatMap(substringPattern -> {
                    Set<String> matchedTopics = matchBySubstring(allTopics, substringPattern).collect(Collectors.toSet());
                    if (matchedTopics.isEmpty()) {
                        warningConsumer.accept(String.format("Could not find any topic matching %s", substringPattern));
                    }
                    return matchedTopics.stream();
                })
                .collect(Collectors.toSet());
        if (topics.isEmpty()) {
            throw new MissingTopicsException("Found no topics matching any pattern " + String.join(",", substringPatterns));
        }

        return topics;
    }

    private Stream<String> matchBySubstring(Set<String> allTopics, String substringPattern) {
        return allTopics.stream().filter(topic -> topic.contains(substringPattern));
    }

    /**
     * @return a - b
     */
    private Set<String> diff(Set<String> a, Set<String> b) {
        return a.stream().filter(n -> !b.contains(n)).collect(Collectors.toSet());
    }


}
