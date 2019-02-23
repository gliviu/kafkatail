package kt.manual;

import kt.test.*;
import kt.test.TestConsumer.ConsumerResult;
import org.awaitility.Duration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static kt.test.TestConsumer.Field.*;
import static kt.test.TestUtils.createTopic;
import static kt.test.TestUtils.sleep;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.*;

@DisplayName("Manual tests")
@ExtendWith(EmbeddedKafkaJunitExtension.class)
class ManualTests {


    @Test
    @Disabled
    void test(TestInfo testInfo) {
        List<String> topics = new ArrayList<>();
        int TOPIC_NO = 10;
        int PARTITION_NO = 3;
        for (int i = 0; i < TOPIC_NO; i++) {
            topics.add(createTopic("topic" + i, testInfo, PARTITION_NO));
        }

        AtomicBoolean stopProducer = new AtomicBoolean();
        CompletableFuture<Void> producer = CompletableFuture.runAsync(() -> {
            for (int i = 0; stopProducer.get() == false; i++) {
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
                .topics(topics).build());
        tc.startConsumer();
        TestUtils.sleep(TEN_SECONDS);
        stopProducer.set(true);
        await().atMost(FIVE_SECONDS).until(producer::isDone);
        TestUtils.sleep(TWO_SECONDS);
        ConsumerResult result = tc.stopConsumer();

        System.out.println("Now: " + now);
        System.out.println(result.asText(VALUE, PARTITION, TIMESTAMP));
    }

}
