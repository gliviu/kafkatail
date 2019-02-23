package kt.cli;

import kt.consumer.ConsumerOptions;
import kt.consumer.MultiConsumer;
import kt.test.EmbeddedKafkaJunitExtension;
import kt.test.TestProducer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static kt.test.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Duration.TWO_SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@DisplayName("Cli")
@ExtendWith(EmbeddedKafkaJunitExtension.class)
@Execution(ExecutionMode.CONCURRENT)
public class CliTests {

    static class Mocks {
        InfoPrinter infoPrinterMock = spy(new InfoPrinter());
        RecordPrinter recordPrinterMock = spy(new RecordPrinter());
        MultiConsumer consumerMock = mock(MultiConsumer.class);
        KafkaAdmin kafkaAdmin = spy(new KafkaAdmin());
    }

    @DisplayName("parses cli arguments")
    @Test
    void t2187() {
        Mocks mocks = new Mocks();
        Mockito.doReturn(set("topic1", "topic2")).when(mocks.kafkaAdmin).allTopics(any());
        Cli cli = cli(mocks);

        int exitCode = cli.run(args("-b localhost:9093 topic1 topic2"));

        assertThat(exitCode).isEqualTo(0);
        ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
        verify(mocks.consumerMock).start(ac.capture(), any(), any());
        assertThat(options(ac.getValue()))
                .isEqualTo("{broker=localhost:9093, topics=[topic1,topic2], start=n/a, end=n/a}");
    }

    @DisplayName("consumes records")
    @Test
    void t3418(TestInfo testInfo) throws ExecutionException, InterruptedException {
        String topic1 = createTopic("topic1", testInfo);

        InfoPrinter infoPrinter = spy(new InfoPrinter());
        RecordPrinter recordPrinter = spy(new RecordPrinter());
        MultiConsumer consumer = spy(new MultiConsumer());
        KafkaAdmin kafkaAdmin = spy(new KafkaAdmin());
        ConsumerOptionsBuilder consumerOptionsBuilder = spy(new ConsumerOptionsBuilder(infoPrinter, kafkaAdmin));
        Cli cli = new Cli(infoPrinter, recordPrinter, consumer, consumerOptionsBuilder);

        CompletableFuture<Integer> futureExitCode =
                CompletableFuture.supplyAsync(() -> cli.run(args("-b localhost:9093 " + topic1)));
        sleep(THREE_SECONDS);
        TestProducer.produce(topic1, "v1");
        sleep(TEN_MILLISECONDS);
        TestProducer.produce(topic1, "v2");
        sleep(TEN_MILLISECONDS);
        TestProducer.produce(topic1, "v3");
        sleep(TWO_SECONDS);
        consumer.stop();
        Awaitility.await().atMost(THREE_SECONDS).until(futureExitCode::isDone);

        assertThat(futureExitCode.get()).isEqualTo(0);
        verify(infoPrinter, atLeast(1)).cliEventReceived(any(CliEvent.class), any(), any());
        verify(recordPrinter, times(3)).printRecords(any());
    }


    @Nested
    @DisplayName("Broker")
    class Broker {
        @DisplayName("defaults missing broker to localhost:9092")
        @Test
        void t3574() {
            Mocks mocks = new Mocks();
            Mockito.doReturn(set("topic1")).when(mocks.kafkaAdmin).allTopics(any());
            Cli cli = cli(mocks);

            int exitCode = cli.run(args("topic1"));

            assertThat(exitCode).isEqualTo(0);
            ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
            verify(mocks.consumerMock).start(ac.capture(), any(), any());
            assertThat(options(ac.getValue()))
                    .isEqualTo("{broker=localhost:9092, topics=[topic1], start=n/a, end=n/a}");
        }

        @DisplayName("defaults missing port to 9092")
        @Test
        void t5219() {
            Mocks mocks = new Mocks();
            Mockito.doReturn(set("topic1")).when(mocks.kafkaAdmin).allTopics(any());
            Cli cli = cli(mocks);

            int exitCode = cli.run(args("-b broker topic1"));

            assertThat(exitCode).isEqualTo(0);
            ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
            verify(mocks.consumerMock).start(ac.capture(), any(), any());
            assertThat(options(ac.getValue()))
                    .isEqualTo("{broker=broker:9092, topics=[topic1], start=n/a, end=n/a}");
        }
    }

    @Nested
    @DisplayName("Consumer limits")
    class ConsumerLimits {
        @DisplayName("accepts start/end consumer limits")
        @Test
        void t9847() {
            Mocks mocks = new Mocks();
            Mockito.doReturn(set("topic2")).when(mocks.kafkaAdmin).allTopics(any());
            Cli cli = cli(mocks);
            int exitCode = cli.run(args("-b broker -s 2015-01-01T10:00 -u 2015-01-01T12:00:00 topic2"));
            assertThat(exitCode).isEqualTo(0);
            ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
            verify(mocks.consumerMock).start(ac.capture(), any(), any());
            assertThat(options(ac.getValue()))
                    .isEqualTo("" +
                            "{broker=broker:9092, topics=[topic2], " +
                            "start=2015-01-01T10:00, end=2015-01-01T12:00:01}");
        }

        @DisplayName("accepts long option of start/end consumer limits")
        @Test
        void t6524() {
            Mocks mocks = new Mocks();
            Mockito.doReturn(set("topic2")).when(mocks.kafkaAdmin).allTopics(any());
            Cli cli = cli(mocks);
            int exitCode = cli.run(args("-b broker --since 2015-01-01T10:00 -until 2015-01-01T12:00:00 topic2"));
            assertThat(exitCode).isEqualTo(0);
            ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
            verify(mocks.consumerMock).start(ac.capture(), any(), any());
            assertThat(options(ac.getValue()))
                    .isEqualTo("" +
                            "{broker=broker:9092, topics=[topic2], " +
                            "start=2015-01-01T10:00, end=2015-01-01T12:00:01}");
        }

        @DisplayName("accepts start consumer limit")
        @Test
        void t3487() {
            Mocks mocks = new Mocks();
            Mockito.doReturn(set("topic2")).when(mocks.kafkaAdmin).allTopics(any());
            Cli cli = cli(mocks);
            int exitCode = cli.run(args("-b broker -s 2015-01-01T10:00 topic2"));
            assertThat(exitCode).isEqualTo(0);
            ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
            verify(mocks.consumerMock).start(ac.capture(), any(), any());
            assertThat(options(ac.getValue()))
                    .isEqualTo("{broker=broker:9092, topics=[topic2], start=2015-01-01T10:00, end=n/a}");
        }

        @DisplayName("accepts start and amount consumer limits")
        @Test
        void t6521() {
            Mocks mocks = new Mocks();
            Cli cli = cli(mocks);
            Mockito.doReturn(set("topic2")).when(mocks.kafkaAdmin).allTopics(any());
            int exitCode = cli.run(args("-b broker -s 2015-01-01T10:00 -a 15m topic2"));
            assertThat(exitCode).isEqualTo(0);
            ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
            verify(mocks.consumerMock).start(ac.capture(), any(), any());
            assertThat(options(ac.getValue()))
                    .isEqualTo("{broker=broker:9092, topics=[topic2], start=2015-01-01T10:00, end=2015-01-01T10:15:01}");
        }

        @DisplayName("accepts long option of amount")
        @Test
        void t4792() {
            Mocks mocks = new Mocks();
            Cli cli = cli(mocks);
            Mockito.doReturn(set("topic2")).when(mocks.kafkaAdmin).allTopics(any());
            int exitCode = cli.run(args("-b broker -s 2015-01-01T10:00 --amount 15m topic2"));
            assertThat(exitCode).isEqualTo(0);
            ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
            verify(mocks.consumerMock).start(ac.capture(), any(), any());
            assertThat(options(ac.getValue()))
                    .isEqualTo("{broker=broker:9092, topics=[topic2], start=2015-01-01T10:00, end=2015-01-01T10:15:01}");
        }

        @DisplayName("rejects end consumer limit without start")
        @Test
        void t2157() {
            Mocks mocks = new Mocks();
            Cli cli = cli(mocks);
            int exitCode = cli.run(args("-b broker -u 2015-01-01T10:00 topic2"));
            assertThat(exitCode).isEqualTo(2);
        }

        @DisplayName("rejects consumer duration limit without start")
        @Test
        void t3248() {
            Cli cli = cli();
            int exitCode = cli.run(args("-b broker -a 20m topic2"));
            assertThat(exitCode).isEqualTo(2);
        }

        @DisplayName("rejects end consumer limit that is before start")
        @Test
        void t3278() {
            Cli cli = cli();
            int exitCode = cli.run(args("-b broker -s 2015-01-01T10:00 -u 2015-01-01T08:00:00 topic2 topic3"));
            assertThat(exitCode).isEqualTo(2);
        }

        @DisplayName("rejects consumer start limit in the future")
        @Test
        void t2758() {
            Mocks mocks = new Mocks();
            Cli cli = cli(mocks);
            LocalDateTime start = LocalDateTime.now().plus(Duration.ofMinutes(10));
            int exitCode = cli.run(args(String.format("-b broker -s %s topic2", start)));
            assertThat(exitCode).isEqualTo(2);
        }

        @DisplayName("rejects consumer end limit in the future")
        @Test
        void t1275() {
            Mocks mocks = new Mocks();
            Cli cli = cli(mocks);
            LocalDateTime end = LocalDateTime.now().plus(Duration.ofMinutes(10));
            int exitCode = cli.run(args(String.format(
                    "-b broker -s 2015-01-01T10:00 -u %s topic2", end)));
            assertThat(exitCode).isEqualTo(2);
        }

        @DisplayName("rejects usage of -u and -a options at the same time")
        @Test
        void t6715() {
            Mocks mocks = new Mocks();
            Cli cli = cli(mocks);
            int exitCode = cli.run(args(
                    "-b broker -s 2015-01-01T10:00 -u 2015-01-01T11:00 -a 10m topic2"));
            assertThat(exitCode).isEqualTo(2);
        }

    }


    @Nested
    @DisplayName("Topics")
    class Topics {
        @DisplayName("consumes all topics if no topic is specified")
        @Test
        void t3576(TestInfo testInfo) {
            createTopic("topic1", testInfo);
            createTopic("topic2", testInfo);
            createTopic("topic3", testInfo);

            Mocks mocks = new Mocks();
            Cli cli = cli(mocks);
            int exitCode = cli.run(args("-b localhost:9093"));

            assertThat(exitCode).isEqualTo(0);
            ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
            verify(mocks.consumerMock).start(ac.capture(), any(), any());
            assertThat(ac.getValue().topics.size()).isGreaterThanOrEqualTo(3);
        }

        @DisplayName("reports unknown topics")
        @Test
        void t9227(TestInfo testInfo) {
            createTopic("topic1", testInfo);
            createTopic("topic2", testInfo);
            Cli cli = cliWithRealConsumer(new MultiConsumer());

            int exitCode = cli.run(args("-b localhost:9093 missing-topic"));

            assertThat(exitCode).isEqualTo(3);
        }

        @DisplayName("accepts topics topic section delimiter")
        @Test
        void t1567() {
            Mocks mocks = new Mocks();
            Mockito.doReturn(set("topic1", "topic2")).when(mocks.kafkaAdmin).allTopics(any());
            Cli cli = cli(mocks);

            int exitCode = cli.run(args("-b localhost:9092 -- topic1 topic2"));

            assertThat(exitCode).isEqualTo(0);
            ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
            verify(mocks.consumerMock).start(ac.capture(), any(), any());
            assertThat(options(ac.getValue()))
                    .isEqualTo("{broker=localhost:9092, topics=[topic1,topic2], start=n/a, end=n/a}");
        }

        @Nested
        @DisplayName("Include Topics By Substring")
        class IncludeTopicsBySubstring {
            @DisplayName("matches topics by substring pattern")
            @Test
            void t7516() {
                Mocks mocks = new Mocks();
                String topics = "topic-gamma-name topicalphaname topic_alpha_name topic-alpha-name topic-beta-name ";
                Mockito.doReturn(set(args(topics))).when(mocks.kafkaAdmin).allTopics(any());
                Cli cli = cli(mocks);

                int exitCode = cli.run(args("-b localhost:9092 -- alpha"));

                assertThat(exitCode).isEqualTo(0);
                ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
                verify(mocks.consumerMock).start(ac.capture(), any(), any());
                assertThat(options(ac.getValue()))
                        .isEqualTo("{broker=localhost:9092, topics=[topic-alpha-name,topic_alpha_name,topicalphaname], start=n/a, end=n/a}");
            }

            @DisplayName("matches topics by multiple substring patterns")
            @Test
            void t3481() {
                Mocks mocks = new Mocks();
                String topics = "topic-gamma-name topicalphaname topic_alpha_name topic-alpha-name topic-beta-name ";
                Mockito.doReturn(set(args(topics))).when(mocks.kafkaAdmin).allTopics(any());
                Cli cli = cli(mocks);

                int exitCode = cli.run(args("-b localhost:9092 -- beta gamma"));

                assertThat(exitCode).isEqualTo(0);
                ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
                verify(mocks.consumerMock).start(ac.capture(), any(), any());
                assertThat(options(ac.getValue()))
                        .isEqualTo("{broker=localhost:9092, topics=[topic-beta-name,topic-gamma-name], start=n/a, end=n/a}");
            }

            @DisplayName("matches topics by full name")
            @Test
            void t5127() {
                Mocks mocks = new Mocks();
                String topics = "topic-gamma-name topicalphaname topic_alpha_name topic-alpha-name topic-beta-name ";
                Mockito.doReturn(set(args(topics))).when(mocks.kafkaAdmin).allTopics(any());
                Cli cli = cli(mocks);

                int exitCode = cli.run(args("-b localhost:9092 -- topicalphaname topic-beta-name"));

                assertThat(exitCode).isEqualTo(0);
                ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
                verify(mocks.consumerMock).start(ac.capture(), any(), any());
                assertThat(options(ac.getValue()))
                        .isEqualTo("{broker=localhost:9092, topics=[topic-beta-name,topicalphaname], start=n/a, end=n/a}");
            }

        }


        @Nested
        @DisplayName("Exclude Topics By Substring")
        class ExcludeTopicsBySubstring {
            @DisplayName("accepts topic exclude parameter before topics")
            @Test
            void t6284() {
                Mocks mocks = new Mocks();
                Mockito.doReturn(set("topic0", "topic1", "topic3")).when(mocks.kafkaAdmin).allTopics(any());
                Cli cli = cli(mocks);

                int exitCode = cli.run(args("-x 1,2 topic1 topic0 topic3"));

                assertThat(exitCode).isEqualTo(0);
                ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
                verify(mocks.consumerMock).start(ac.capture(), any(), any());
                assertThat(options(ac.getValue()))
                        .isEqualTo("{broker=localhost:9092, topics=[topic0,topic3], start=n/a, end=n/a}");
            }

            @DisplayName("accepts topic exclude parameter after topics")
            @Test
            void t4915() {
                Mocks mocks = new Mocks();
                Mockito.doReturn(set("topic1", "topic0", "topic3")).when(mocks.kafkaAdmin).allTopics(any());
                Cli cli = cli(mocks);

                int exitCode = cli.run(args("topic1 topic0 topic3 -x 1,2"));

                assertThat(exitCode).isEqualTo(0);
                ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
                verify(mocks.consumerMock).start(ac.capture(), any(), any());
                assertThat(options(ac.getValue()))
                        .isEqualTo("{broker=localhost:9092, topics=[topic0,topic3], start=n/a, end=n/a}");
            }

            @DisplayName("excludes topics from all topics available in kafka broker")
            @Test
            void t3487() {
                Mocks mocks = new Mocks();
                String topics = "topic-alpha-name topic-beta-name";
                Mockito.doReturn(set(args(topics))).when(mocks.kafkaAdmin).allTopics(any());
                Cli cli = cli(mocks);

                int exitCode = cli.run(args("" +
                        "-b localhost:9093 -x alpha"));

                assertThat(exitCode).isEqualTo(0);
                ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
                verify(mocks.consumerMock).start(ac.capture(), any(), any());
                Set<String> actualTopics = ac.getValue().topics;
                assertThat(actualTopics).contains("topic-beta-name");
                assertThat(actualTopics).doesNotContain("topic-alpha-name");
            }

            @DisplayName("excludes topics by substring")
            @Test
            void t6278() {
                Mocks mocks = new Mocks();
                String topics = "topicalphaname topic_alpha_name topic-alpha-name topic-beta-name ";
                Mockito.doReturn(set(args(topics))).when(mocks.kafkaAdmin).allTopics(any());
                Cli cli = cli(mocks);

                int exitCode = cli.run(args("" +
                        "-b localhost:9093 " +
                        topics +
                        "-x alpha"));

                assertThat(exitCode).isEqualTo(0);
                ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
                verify(mocks.consumerMock).start(ac.capture(), any(), any());
                assertThat(ac.getValue().topics).contains("topic-beta-name");
                assertThat(ac.getValue().topics).doesNotContain("topic_alpha_name", "topicalphaname", "topic-alpha-name");
            }

            @DisplayName("excludes topics by  multiple exclude substrings")
            @Test
            void t5782() {
                Mocks mocks = new Mocks();
                String topics = "topicalphaname topic_alpha_name topic-beta-name topic-gamma-name";
                Mockito.doReturn(set(args(topics))).when(mocks.kafkaAdmin).allTopics(any());
                Cli cli = cli(mocks);

                int exitCode = cli.run(args("" +
                        " -b localhost:9093 " +
                        topics +
                        " -x alpha,gamma"));

                assertThat(exitCode).isEqualTo(0);
                ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
                verify(mocks.consumerMock).start(ac.capture(), any(), any());
                assertThat(ac.getValue().topics).contains("topic-beta-name");
                assertThat(ac.getValue().topics).doesNotContain("topic_alpha_name", "topic-gamma-name");
            }

            @DisplayName("excludes topics by full name")
            @Test
            void t4732() {
                Mocks mocks = new Mocks();
                String topics = "topicalphaname topic_alpha_name topic-beta-name topic-gamma-name";
                Mockito.doReturn(set(args(topics))).when(mocks.kafkaAdmin).allTopics(any());
                Cli cli = cli(mocks);

                int exitCode = cli.run(args("" +
                        " -b localhost:9093 " +
                        topics +
                        " -x topic_alpha_name,topic-beta-name"));

                assertThat(exitCode).isEqualTo(0);
                ArgumentCaptor<ConsumerOptions> ac = ArgumentCaptor.forClass(ConsumerOptions.class);
                verify(mocks.consumerMock).start(ac.capture(), any(), any());
                assertThat(ac.getValue().topics).contains("topicalphaname", "topic-gamma-name");
                assertThat(ac.getValue().topics).doesNotContain("topic_alpha_name","topic-beta-name");
            }

            @DisplayName("reports exclusion patterns that excludes all topics")
            @Test
            void t7598() {
                Mocks mocks = new Mocks();
                String topics = "topic1 topic2";
                Mockito.doReturn(set(args(topics))).when(mocks.kafkaAdmin).allTopics(any());
                Cli cli = cli(mocks);

                int exitCode = cli.run(args("" +
                        " -b localhost:9093 -x top -- " + topics));

                assertThat(exitCode).isEqualTo(2);
            }

        }
    }

    private Cli cli(Mocks mocks) {
        ConsumerOptionsBuilder consumerOptionsBuilder =
                new ConsumerOptionsBuilder(mocks.infoPrinterMock, mocks.kafkaAdmin);
        return new Cli(mocks.infoPrinterMock, mocks.recordPrinterMock, mocks.consumerMock, consumerOptionsBuilder);
    }

    private Cli cli() {
        Mocks mocks = new Mocks();
        return cli(mocks);
    }

    private Cli cliWithRealConsumer(MultiConsumer consumer) {
        Mocks mocks = new Mocks();
        ConsumerOptionsBuilder consumerOptionsBuilder =
                new ConsumerOptionsBuilder(mocks.infoPrinterMock, mocks.kafkaAdmin);
        return new Cli(mocks.infoPrinterMock, mocks.recordPrinterMock, consumer, consumerOptionsBuilder);
    }

    private String options(ConsumerOptions options) {
        String topics = options.topics.stream()
                .sorted()
                .collect(Collectors.joining(","));
        topics = "[" + topics + "]";
        String start = options.startConsumerLimit == null ? "n/a" : LocalDateTime.ofInstant(options.startConsumerLimit, ZoneId.systemDefault()).toString();
        String end = options.endConsumerLimit == null ? "n/a" : LocalDateTime.ofInstant(options.endConsumerLimit, ZoneId.systemDefault()).toString();
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("broker", options.broker);
        map.put("topics", topics);
        map.put("start", start);
        map.put("end", end);
        return map.toString();
    }

    String[] args(String args) {
        return args.split(" ");
    }

    Set<String> set(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }

}
