package kt.consumer;

import kt.test.*;
import kt.test.TestConsumer.ConsumerResult;
import kt.test.TestProducer.Stoppable;
import org.assertj.core.api.Assertions;
import org.awaitility.Duration;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static kt.test.TestConsumer.Field.*;
import static kt.test.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.*;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@Execution(CONCURRENT)
@DisplayName("Multi Consumer")
@ExtendWith(EmbeddedKafkaJunitExtension.class)
class TestMultiConsumer {

    @DisplayName("consumes new records")
    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void t4728(Boolean ordered, TestInfo testInfo) throws URISyntaxException, IOException {
        String topic = createTopic("topic1_" + UUID.randomUUID(), testInfo);
        TestConsumer tc = new TestConsumer(OptionBuilder.options()
                .topics(topic)
                .sortRecords(ordered).build());

        tc.startConsumer();

        TestProducer.produce(topic, "key1", "val1");
        TestProducer.produce(topic, "key2", "val2");
        TestProducer.produce(topic, "key3", "val3");
        await().atMost(TEN_SECONDS).untilAtomic(tc.stats.recordCount, greaterThanOrEqualTo(3));
        TestUtils.sleep(TWO_SECONDS);
        ConsumerResult result = tc.stopConsumer();

        String actual = result.asText(KEY, VALUE);
        String expected = expected(testInfo);
        assertThat(actual).isEqualTo(expected);
    }

    @DisplayName("does not consume prior records")
    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void t6915(Boolean ordered, TestInfo testInfo) throws URISyntaxException, IOException {
        String topic = createTopic("topic1_" + UUID.randomUUID(), testInfo);
        TestConsumer tc = new TestConsumer(OptionBuilder.options()
                .topics(topic)
                .sortRecords(ordered).build());

        TestProducer.produce(topic, "val1");
        TestProducer.produce(topic, "val2");
        TestProducer.produce(topic, "val3");
        TestUtils.sleep(ONE_SECOND);
        tc.startConsumer();
        TestProducer.produce(topic, "val4");
        TestProducer.produce(topic, "val5");
        TestProducer.produce(topic, "val6");
        await().atMost(TEN_SECONDS).untilAtomic(tc.stats.recordCount, greaterThanOrEqualTo(3));
        TestUtils.sleep(TWO_SECONDS);
        ConsumerResult result = tc.stopConsumer();

        String actual = result.asText(VALUE);
        String expected = expected(testInfo);
        assertThat(actual).isEqualTo(expected);
    }

    @DisplayName("accepts records with no key")
    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void t8472(Boolean ordered, TestInfo testInfo) throws URISyntaxException, IOException {
        String topic = createTopic("topic1_" + UUID.randomUUID(), testInfo);
        TestConsumer tc = new TestConsumer(OptionBuilder.options()
                .topics(topic)
                .sortRecords(ordered).build());

        tc.startConsumer();

        TestProducer.produce(topic, "val1");
        TestProducer.produce(topic, "val2");
        TestProducer.produce(topic, "val3");
        await().atMost(TEN_SECONDS).untilAtomic(tc.stats.recordCount, greaterThanOrEqualTo(3));
        TestUtils.sleep(TWO_SECONDS);
        ConsumerResult result = tc.stopConsumer();

        String actual = result.asText(VALUE);
        String expected = expected(testInfo);
        assertThat(actual).isEqualTo(expected);
    }

    @DisplayName("consumes from multiple topics")
    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void t8467(Boolean ordered, TestInfo testInfo) throws URISyntaxException, IOException {
        String topic1 = createTopic("topic1_" + UUID.randomUUID(), testInfo);
        String topic2 = createTopic("topic2_" + UUID.randomUUID(), testInfo);
        String topic3 = createTopic("topic3_" + UUID.randomUUID(), testInfo);
        TestConsumer tc = new TestConsumer(OptionBuilder.options()
                .topics(topic1, topic2, topic3)
                .sortRecords(ordered).build());

        tc.startConsumer();

        TestProducer.produce(topic1, "key1", "val1");
        TestProducer.produce(topic2, "key2", "val2");
        TestProducer.produce(topic1, "key3", "val3");
        TestProducer.produce(topic3, "key4", "val4");
        TestProducer.produce(topic1, "key5", "val5");
        TestProducer.produce(topic2, "key6", "val6");
        await().atMost(TEN_SECONDS).untilAtomic(tc.stats.recordCount, greaterThanOrEqualTo(6));
        TestUtils.sleep(TWO_SECONDS);
        ConsumerResult result = tc.stopConsumer();

        String actual = result
                .sorted(ordered)
                .asText(KEY, VALUE);
        String expected = expected(testInfo);
        assertThat(actual).isEqualTo(expected);
    }

    @DisplayName("consumes from multiple topics with multiple partitions each")
    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void t7246(Boolean ordered, TestInfo testInfo) throws URISyntaxException, IOException {
        String topic1 = createTopic("topic1_" + UUID.randomUUID(), testInfo, 5);
        String topic2 = createTopic("topic2_" + UUID.randomUUID(), testInfo, 5);
        String topic3 = createTopic("topic3_" + UUID.randomUUID(), testInfo, 5);
        TestConsumer tc = new TestConsumer(OptionBuilder.options()
                .topics(topic1, topic2, topic3)
                .sortRecords(ordered).build());

        tc.startConsumer();

        TestProducer.produce(topic1, "val01", 4);
        TestProducer.produce(topic2, "val02", 3);
        TestProducer.produce(topic1, "val03", 1);
        TestProducer.produce(topic3, "val04", 3);
        TestProducer.produce(topic1, "val05", 2);
        TestProducer.produce(topic2, "val06", 1);
        TestProducer.produce(topic3, "val07", 3);
        TestProducer.produce(topic1, "val08", 3);
        TestProducer.produce(topic2, "val09", 2);
        TestProducer.produce(topic3, "val10", 0);
        TestProducer.produce(topic1, "val11", 2);
        TestProducer.produce(topic2, "val12", 0);
        await().atMost(TEN_SECONDS).untilAtomic(tc.stats.recordCount, greaterThanOrEqualTo(12));
        TestUtils.sleep(TWO_SECONDS);
        ConsumerResult result = tc.stopConsumer();

        String actual = result
                .sorted(ordered)
                .asText(VALUE, PARTITION);
        String expected = expected(testInfo);
        assertThat(actual).isEqualTo(expected);
    }

    @DisplayName("does not create consumer group")
    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void t9571(Boolean ordered, TestInfo testInfo) {
        String topic = createTopic("topic1_" + UUID.randomUUID(), testInfo);
        TestConsumer tc = new TestConsumer(OptionBuilder.options()
                .topics(topic)
                .sortRecords(ordered).build());

        tc.startConsumer();
        TestProducer.produce(topic, "val1");
        TestProducer.produce(topic, "val2");
        TestProducer.produce(topic, "val3");
        await().atMost(TEN_SECONDS).untilAtomic(tc.stats.recordCount, greaterThanOrEqualTo(3));
        TestUtils.sleep(TWO_SECONDS);

        assertThat(TestUtils.getConsumerGroups("localhost:9093")).size().isEqualTo(0);
    }

    @Nested
    @DisplayName("Historical records")
    class HistoricalRecords {
        /**
         * <pre>
         * t0a       t2b t2a  t3b       t5a t5b  t6a t6b
         * 0    1    2        3    4    5        6
         *      ^                  ^
         *      |                  |
         *      |                  start consumer
         *      |--------------------------->
         *      -3s
         * </pre>
         */
        @DisplayName("consumes historical records")
        @ParameterizedTest
        @ValueSource(strings = {"true", "false"})
        void t9472(Boolean ordered, TestInfo testInfo) throws URISyntaxException, IOException {
            String topic1 = createTopic("topic1_" + UUID.randomUUID(), testInfo);
            String topic2 = createTopic("topic2_" + UUID.randomUUID(), testInfo);

            TestProducer.produce(topic1, "t0a");
            TestUtils.sleep(TWO_SECONDS);
            TestProducer.produce(topic1, "t2a");
            TestProducer.produce(topic2, "t2b");
            TestUtils.sleep(ONE_SECOND);
            TestProducer.produce(topic2, "t3b");
            TestUtils.sleep(ONE_SECOND);
            TestConsumer tc = new TestConsumer(OptionBuilder.options()
                    .startConsumerLimit(Instant.now().minusMillis(THREE_SECONDS.getValueInMS()))
                    .topics(topic1, topic2)
                    .sortRecords(ordered).build());
            tc.startConsumer();
            TestUtils.sleep(ONE_SECOND);
            TestProducer.produce(topic1, "t5a");
            TestProducer.produce(topic1, "t5b");

            TestUtils.sleep(ONE_SECOND);
            TestProducer.produce(topic1, "t6a");
            TestProducer.produce(topic1, "t6b");

            await().atMost(TEN_SECONDS).untilAtomic(tc.stats.recordCount, greaterThanOrEqualTo(3));
            TestUtils.sleep(TWO_SECONDS);
            ConsumerResult result = tc.stopConsumer();

            String actual = result
                    .sorted(ordered)
                    .asText(VALUE);
            String expected = expected(testInfo);
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * <pre>
         *       t0   t1        t3
         *       0    1    2    3
         * ^               ^
         * |               |
         * |         -     start consumer
         * |--------------------------->
         * -10s
         * </pre>
         */
        @DisplayName("consumes historical records from beginning of stream")
        @ParameterizedTest
        @ValueSource(strings = {"true", "false"})
        void t2648(Boolean ordered, TestInfo testInfo) throws URISyntaxException, IOException {
            String topic1 = createTopic("topic1_" + UUID.randomUUID(), testInfo);
            String topic2 = createTopic("topic2_" + UUID.randomUUID(), testInfo);

            TestProducer.produce(topic1, "t0");
            TestUtils.sleep(ONE_SECOND);
            TestProducer.produce(topic2, "t1");
            TestUtils.sleep(ONE_SECOND);
            TestConsumer tc = new TestConsumer(OptionBuilder.options()
                    .startConsumerLimit(Instant.now().minusMillis(TEN_SECONDS.getValueInMS()))
                    .topics(topic1, topic2)
                    .sortRecords(ordered).build());
            tc.startConsumer();
            TestUtils.sleep(ONE_SECOND);
            TestProducer.produce(topic2, "t3");

            await().atMost(TEN_SECONDS).untilAtomic(tc.stats.recordCount, greaterThanOrEqualTo(3));
            TestUtils.sleep(TWO_SECONDS);
            ConsumerResult result = tc.stopConsumer();

            String actual = result
                    .sorted(ordered)
                    .asText(VALUE);
            String expected = expected(testInfo);
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * <pre>
         * t0        t2   t3                t7
         * 0    1    2    3    4    5   6   7
         *      ^         ^             ^
         *      |         |             |
         *      |<------->|             start consumer
         *      -6s       -3s
         * </pre>
         */
        @DisplayName("consumes historical records within interval in the past")
        @ParameterizedTest
        @ValueSource(strings = {"true", "false"})
        void t4742(Boolean ordered, TestInfo testInfo) throws URISyntaxException, IOException {
            String topic1 = createTopic("topic1_" + UUID.randomUUID(), testInfo);
            String topic2 = createTopic("topic2_" + UUID.randomUUID(), testInfo);

            TestProducer.produce(topic1, "t0");
            TestUtils.sleep(TWO_SECONDS);
            TestProducer.produce(topic1, "t2");
            TestUtils.sleep(ONE_SECOND);
            TestProducer.produce(topic2, "t3");
            TestUtils.sleep(THREE_SECONDS);
            TestConsumer tc = new TestConsumer(OptionBuilder.options()
                    .startConsumerLimit(Instant.now().minusMillis(FIVE_SECONDS.getValueInMS()))
                    .endConsumerLimit(Instant.now().minusMillis(THREE_SECONDS.getValueInMS()))
                    .topics(topic1, topic2)
                    .sortRecords(ordered).build());
            tc.startConsumer();
            TestUtils.sleep(ONE_SECOND);
            TestProducer.produce(topic2, "t7");

            await().atMost(FIVE_SECONDS).untilAtomic(tc.stats.recordCount, greaterThanOrEqualTo(2));
            TestUtils.sleep(TWO_SECONDS);
            ConsumerResult result = tc.stopConsumer();

            String actual = result
                    .sorted(ordered)
                    .asText(VALUE);
            String expected = expected(testInfo);
            assertThat(actual).isEqualTo(expected);
        }

        @DisplayName("uses this formula for historical records: record_timestamp >= start && record_timestamp <= end")
        @RepeatedTest(10)
        void t6587(TestInfo testInfo, RepetitionInfo repetitionInfo) {
            String topic = createTopic(testInfo, repetitionInfo);
            TestConsumer allRecordsConsumer = new TestConsumer(OptionBuilder.options()
                    .topics(topic).build());
            allRecordsConsumer.startConsumer();
            Stoppable producer = TestProducer.produceRate(topic, new Duration(10, TimeUnit.MILLISECONDS));

            sleep(THREE_SECONDS);
            Instant start = Instant.now().minusMillis(TWO_SECONDS.getValueInMS());
            Instant end = Instant.now().minusMillis(TWO_HUNDRED_MILLISECONDS.getValueInMS());
            TestConsumer tc = new TestConsumer(OptionBuilder.options()
                    .startConsumerLimit(start)
                    .endConsumerLimit(end)
                    .topics(topic).build());
            tc.startConsumer();
            sleep(THREE_SECONDS);
            producer.stop();
            ConsumerResult actual = tc.awaitStopConsumer();
            long actualCount = actual.count();
            ConsumerResult allRecords = allRecordsConsumer.stopConsumer();
            long expectedCount = allRecords.filtered(record -> record.timestamp() >= start.toEpochMilli()
                    && record.timestamp() <= end.toEpochMilli())
                    .count();
            assertThat(actualCount).isEqualTo(expectedCount);
        }

        @DisplayName("consumes historical records from multiple topics with multiple partitions each")
        @ParameterizedTest
        @ValueSource(strings = {"true", "false"})
        void t3475(Boolean ordered, TestInfo testInfo) throws URISyntaxException, IOException {
            String topic1 = createTopic("topic1_" + UUID.randomUUID(), testInfo, 5);
            String topic2 = createTopic("topic2_" + UUID.randomUUID(), testInfo, 5);
            String topic3 = createTopic("topic3_" + UUID.randomUUID(), testInfo, 5);
            TestConsumer tc = new TestConsumer(OptionBuilder.options()
                    .startConsumerLimit(Instant.now())
                    .topics(topic1, topic2, topic3)
                    .sortRecords(ordered).build());

            sleep(ONE_SECOND);
            TestProducer.produce(topic1, "val01", 4);
            TestProducer.produce(topic2, "val02", 3);
            TestProducer.produce(topic1, "val03", 1);
            TestProducer.produce(topic3, "val04", 3);
            TestProducer.produce(topic1, "val05", 2);
            TestProducer.produce(topic2, "val06", 1);
            TestProducer.produce(topic3, "val07", 3);
            TestProducer.produce(topic1, "val08", 3);
            TestProducer.produce(topic2, "val09", 2);
            TestProducer.produce(topic3, "val10", 0);
            TestProducer.produce(topic1, "val11", 2);
            TestProducer.produce(topic2, "val12", 0);

            tc.startConsumer();

            await().atMost(TEN_SECONDS).untilAtomic(tc.stats.recordCount, greaterThanOrEqualTo(12));
            TestUtils.sleep(TWO_SECONDS);
            ConsumerResult result = tc.stopConsumer();

            String actual = result
                    .sorted(ordered)
                    .asText(VALUE, PARTITION);
            String expected = expected(testInfo);
            assertThat(actual).isEqualTo(expected);
        }

        @DisplayName("consumes historical records (load test)")
        @ParameterizedTest
        @ValueSource(strings = {"true", "false"})
        void t7591(Boolean ordered, TestInfo testInfo) {
            List<String> topics = new ArrayList<>();
            int TOPIC_NO = 10;
            int PARTITION_NO = 3;
            for (int i = 0; i < TOPIC_NO; i++) {
                topics.add(createTopic("topic" + i + "_" + UUID.randomUUID(), testInfo, PARTITION_NO));
            }

            AtomicBoolean stopProducer = new AtomicBoolean();
            CompletableFuture<Void> producer = CompletableFuture.runAsync(() -> {
                for (int i = 0; !stopProducer.get(); i++) {
                    TestProducer.produce(
                            topics.get((int) (Math.random() * TOPIC_NO)),
                            Integer.toString(i),
                            (int) (Math.random() * PARTITION_NO));
                    sleep(new Duration(10, TimeUnit.MILLISECONDS));
                }
            });

            TestUtils.sleep(TEN_SECONDS);
            Instant now = Instant.now();
            TestUtils.sleep(TEN_SECONDS);
            TestConsumer tc = new TestConsumer(OptionBuilder.options()
                    .startConsumerLimit(now)
                    .topics(topics)
                    .sortRecords(ordered).build());
            tc.startConsumer();
            TestUtils.sleep(TEN_SECONDS);
            stopProducer.set(true);
            await().atMost(TEN_SECONDS).until(producer::isDone);
            TestUtils.sleep(TWO_SECONDS);
            ConsumerResult result = tc.stopConsumer();


            Stream<String> values = result
                    .sorted(ordered)
                    .asValues();
            Stats stats = stats(values);
            org.assertj.core.api.Assertions.assertThat(stats.isContiguous).isTrue();
            org.assertj.core.api.Assertions.assertThat(stats.hasDuplicates).isFalse();
            long minTimestamp = result.asTimestamps().mapToLong(Instant::toEpochMilli).min().orElse(-1);
            Assertions.assertThat(minTimestamp).isGreaterThanOrEqualTo(now.toEpochMilli());
        }

    }

    static class Stats {
        boolean hasDuplicates;
        boolean isContiguous;

        Stats(boolean hasDuplicates, boolean isContiguous) {
            this.hasDuplicates = hasDuplicates;
            this.isContiguous = isContiguous;
        }

        @Override
        public String toString() {
            return "Stats{" +
                    "hasDuplicates=" + hasDuplicates +
                    ", isContiguous=" + isContiguous +
                    '}';
        }
    }

    private Stats stats(Stream<String> values) {
        int[] valuesArray = values.mapToInt(Integer::parseInt).sorted().toArray();
        if (valuesArray.length <= 1) {
            return new Stats(false, true);
        }
        int v1 = valuesArray[0];
        boolean hasDuplicates = false;
        boolean isContiguous = true;
        for (int i = 1; i < valuesArray.length; i++) {
            int v2 = valuesArray[i];
            if (v1 == v2) {
                hasDuplicates = true;
            }
            if (v2 - v1 != 1) {
                isContiguous = false;
            }
            v1 = v2;
        }
        return new Stats(hasDuplicates, isContiguous);
    }

}
