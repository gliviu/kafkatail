package kt.manual;

import kt.cli.Dates;
import kt.consumer.ConsumerOptions;
import kt.consumer.MultiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static kt.test.TestUtils.THIRTY_SECONDS;
import static kt.test.TestUtils.sleep;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Manual tests")
@Disabled
class ManualTests {

    /**
     * Run with <pre>gradle test -i --tests *t4873</pre>
     */
    @SuppressWarnings({"ConstantConditions"})
    @DisplayName("records are sorted when using sortRecords option")
    @Execution(ExecutionMode.SAME_THREAD)
    @RepeatedTest(100)
    void t4873(TestInfo testInfo, RepetitionInfo repetitionInfo) {
        FileLogger.changeLogName();
        FileLogger.write("START %s %s",
                testInfo.getTestMethod().map(Method::getName).orElse("N/A"), repetitionInfo.getCurrentRepetition());
        String bootstrapEnv = System.getenv("T4873");
        String BOOTSTRAP_SERVERS = bootstrapEnv == null ? "localhost:9092" : bootstrapEnv;
        Duration RUN_TIME = ofSeconds(300);

        // Producer setup
        double FREQUENCY = 0.1; // ie. 5 - will produce records at random intervals between [0, 5] seconds.
        int TOPIC_NO = 20;
        int PARTITION_NO = 10;

        // Consumer configuration
        boolean READ_HISTORICAL_RECORDS_FROM_BEGINNING = true;
        boolean READ_HISTORICAL_RECORDS = false;
        Duration READ_HISTORICAL_RECORDS_DELAY = ofSeconds(50);

        Instant startTime = now();
        ManualTestProducer producer = new ManualTestProducer(BOOTSTRAP_SERVERS, TOPIC_NO, PARTITION_NO, READ_HISTORICAL_RECORDS_FROM_BEGINNING);
        MultiConsumer consumer = new MultiConsumer();
        ConsumerOptions options = new ConsumerOptions();
        options.broker = BOOTSTRAP_SERVERS;
        options.sortRecords = true;
        options.startConsumerLimit = READ_HISTORICAL_RECORDS ? startTime : null;
        options.fromBeginning = READ_HISTORICAL_RECORDS_FROM_BEGINNING;
        options.topics = producer.getTopics();

        CompletableFuture.runAsync(() -> producer.start(FREQUENCY, Duration.ofSeconds(1)));

        System.out.println("=====   BOOTSTRAP_SERVERS: " + BOOTSTRAP_SERVERS);
        System.out.println("=====   ORDERED: " + options.sortRecords);
        System.out.println("=====   FROM BEGINNING: " + options.fromBeginning);
        System.out.println("=====   START: " + options.startConsumerLimit);
        System.out.println("=====   LOGS: " + FileLogger.getlogPath());
        AtomicReference<ConsumerRecord<String, String>> currentValue = new AtomicReference<>();
        AtomicLong consumedRecordCount = new AtomicLong(), orderMismatch = new AtomicLong();
        CompletableFuture.runAsync(() -> {
            try {
                if (READ_HISTORICAL_RECORDS || READ_HISTORICAL_RECORDS_FROM_BEGINNING) {
                    sleep(READ_HISTORICAL_RECORDS_DELAY);
                }
                consumer.start(options, event -> {/*ignore*/}, currentRecord -> {
                    consumedRecordCount.incrementAndGet();
                    FileLogger.write("CONSUMED topic:%s partition:%s value:%s", currentRecord.topic(), currentRecord.partition(), currentRecord.value());
                    ConsumerRecord<String, String> previousRecord = currentValue.get();
                    if (previousRecord != null) {
                        if (previousRecord.timestamp() > currentRecord.timestamp()) {
                            System.out.println(String.format("Order mismatch: %s %s, %s %s",
                                    Dates.localDateTime(Instant.ofEpochMilli(previousRecord.timestamp())), previousRecord.value(),
                                    Dates.localDateTime(Instant.ofEpochMilli(currentRecord.timestamp())), currentRecord.value()));
                            orderMismatch.incrementAndGet();
                        }
                    }
                    currentValue.set(currentRecord);
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        while (Duration.between(startTime, now()).minus(RUN_TIME).isNegative()) {
            sleep(ofSeconds(10));
            if (currentValue.get() == null) {
                System.out.println("none");
                continue;
            }
            ConsumerRecord<String, String> record = currentValue.get();
            System.out.println(String.format("topic: %s, partition: %s, key: %s, value: %s", record.topic(), record.partition(), record.key(), record.value()));
        }

        producer.stop();
        waitConsumer(producer, consumedRecordCount);
        consumer.stop();

        FileLogger.write("END %s %s",
                testInfo.getTestMethod().map(Method::getName).orElse("N/A"), repetitionInfo.getCurrentRepetition());

        assertThat(consumedRecordCount.get()).as("Missing records detected.").isEqualTo(producer.getProducedRecordCount());
        assertThat(orderMismatch.get()).as("Order mismatch detected.").isEqualTo(0L);
    }

    private void waitConsumer(ManualTestProducer producer, AtomicLong consumedRecordCount) {
        try{
            Awaitility.await().atMost(THIRTY_SECONDS).until(() -> producer.getProducedRecordCount() == consumedRecordCount.get());
        } catch(ConditionTimeoutException e) {
            // ignore
        }
    }
}
