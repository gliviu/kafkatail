package kt.test;

import kt.cli.CliEvent;
import kt.consumer.ConsumerOptions;
import kt.consumer.MultiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.FIVE_SECONDS;
import static org.awaitility.Duration.TEN_SECONDS;
import static org.hamcrest.Matchers.equalTo;

public class TestConsumer {
    private final static Logger logger = LoggerFactory.getLogger(TestConsumer.class);
    private static final ExecutorService executor = Executors.newFixedThreadPool(20);
    private static final Pattern NORMALIZE_TOPIC_PATTERN = Pattern.compile("(.*?)_.*");

    public enum Field {
        PARTITION, KEY, VALUE, TIMESTAMP
    }

    public static class ConsumerResult {
        private List<ConsumerRecord<String, String>> records;

        /**
         * Kafka does not provide any ordering guarantee when multiple topics or multiple partitions are consumed.
         * For this reason we enforce sorting for having consistent results when comparing with reference expected values.
         *
         * @param alreadySorted true if this {@link ConsumerResult} is already sorted.
         *                      We do not sort records in this case
         * @return copy of this @{@link ConsumerResult} with records sorted by value if not 'already sorted'.
         * Otherwise returns unaltered result.
         */
        public ConsumerResult sorted(boolean alreadySorted) {
            if (alreadySorted) {
                return this;
            }
            ConsumerResult res = new ConsumerResult();
            res.records = records
                    .stream().sorted(Comparator.comparing(ConsumerRecord::value))
                    .collect(Collectors.toList());
            return res;
        }

        public ConsumerResult filtered(Predicate<ConsumerRecord<String, String>> predicate) {
            ConsumerResult res = new ConsumerResult();
            res.records = records
                    .stream().filter(predicate)
                    .collect(Collectors.toList());
            return res;
        }

        public String asText(Field... fields) {
            return records.stream()
                    .map(record -> recordText(record, fields))
                    .collect(Collectors.joining("\n"));
        }

        public Stream<String> asValues() {
            return records.stream().map(ConsumerRecord::value);
        }

        public Stream<Instant> asTimestamps() {
            return records.stream()
                    .map(ConsumerRecord::timestamp)
                    .map(Instant::ofEpochMilli);
        }

        private String recordText(ConsumerRecord<String, String> record, Field[] fields) {
            Set<Field> fieldSet = new HashSet<>(Arrays.asList(fields));
            List<String> res = new ArrayList<>();
            if (fieldSet.contains(Field.TIMESTAMP)) {
                res.add("timestamp:" + Instant.ofEpochMilli(record.timestamp()));
            }
            res.add("topic:" + normalizeTopicName(record.topic()));
            if (fieldSet.contains(Field.KEY)) {
                res.add("key:" + record.key());
            }
            if (fieldSet.contains(Field.VALUE)) {
                res.add("value:" + record.value());
            }
            if (fieldSet.contains(Field.PARTITION)) {
                res.add("partition:" + record.partition());
            }
            return String.join(", ", res);
        }

        /**
         * Removes suffix starting with underscore if present.
         * <pre>
         * Examples:
         * topic1_a3d223 -> topic1
         * topic1 -> topic1   (leaves unchanged)
         * </pre>
         *
         * @param topic un-normalized topic name
         * @return normalized topic name
         */
        private String normalizeTopicName(String topic) {
            Matcher matcher = NORMALIZE_TOPIC_PATTERN.matcher(topic);
            if (matcher.matches()) {
                return matcher.group(1);
            }
            return topic;
        }

        public long count() {
            return (long) records.size();
        }
    }

    public static class ConsumerStatistics {
        public AtomicInteger recordCount = new AtomicInteger();
        public AtomicBoolean consumerEnded = new AtomicBoolean();
    }

    private MultiConsumer mc = new MultiConsumer();
    public ConsumerStatistics stats = new ConsumerStatistics();
    private ConsumerOptions options;
    @Nullable
    private CompletableFuture<ConsumerResult> future;

    public TestConsumer(ConsumerOptions options) {
        this.options = options;
    }

    /**
     * Starts consuming records in s separate thread.
     * At the end of this call it is guaranteed that consume is already consuming messages.
     */
    public void startConsumer() {
        AtomicBoolean consumerStarted = new AtomicBoolean();
        future = CompletableFuture
                .supplyAsync(() -> {
                    List<ConsumerRecord<String, String>> records = new ArrayList<>();
                    mc.start(options,
                            consumerEvent -> {
                                CliEvent cliEvent = CliEvent.valueOf(consumerEvent.name());
                                switch (cliEvent) {
                                    case START_CONSUME:
                                        consumerStarted.set(true);
                                        break;
                                    case END_CONSUME:
                                        stats.consumerEnded.set(true);
                                        break;
                                }
                                if (cliEvent == CliEvent.START_CONSUME) {
                                    consumerStarted.set(true);
                                }
                            },
                            record -> {
                                System.out.println(String.format("CONSUMED %s %s %s", Instant.ofEpochMilli(record.timestamp()), record.topic(), record.value()));
                                stats.recordCount.incrementAndGet();
                                records.add(record);
                            });
                    return records;
                }, executor)
                .thenApply(records -> {
                    ConsumerResult result = new ConsumerResult();
                    result.records = records;
                    return result;
                })
                .whenComplete((result, error) -> {
                    if (error != null) {
                        logger.error("Error occurred in multi consumer", error);
                    }
                });
        Awaitility.await().atMost(FIVE_SECONDS).untilAsserted(() ->
                assertThat(consumerStarted).describedAs("MultiConsumer did not start consuming").isTrue());
    }

    public ConsumerResult stopConsumer() {
        Objects.requireNonNull(future);
        mc.stop();
        return awaitStopConsumer();
    }

    public ConsumerResult awaitStopConsumer() {
        Objects.requireNonNull(future);
        await().atMost(TEN_SECONDS).untilAtomic(stats.consumerEnded, equalTo(true));
        await().atMost(TEN_SECONDS).until(future::isDone);
        try {
            ConsumerResult result = future.get();
            future = null;
            return result;
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

}
