package kt.test;

import org.apache.kafka.clients.admin.*;
import org.awaitility.Duration;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.TestInfo;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("WeakerAccess")
public class TestUtils {

    public static final Duration TEN_MILLISECONDS = new Duration(10, TimeUnit.MILLISECONDS);
    public static final Duration THREE_SECONDS = new Duration(3, TimeUnit.SECONDS);

    public static String bootstrapServers() {
        return "localhost:9093";
    }

    /**
     * Checks if kafka server is running.
     *
     * @param timeout check is done during this period
     * @return true if server is running; false if server did not start during the timeout period
     */
    public static boolean isServerAvailable(Duration timeout) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (AdminClient ac = AdminClient.create(props)) {
            DescribeClusterResult res = ac.describeCluster(
                    new DescribeClusterOptions().timeoutMs((int) timeout.getValueInMS()));
            res.clusterId().get();
            return true;
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
    }

    public static String createTopic(String prefix, TestInfo testInfo) {
        return createTopic(prefix, testInfo, null, 1);
    }

    public static String createTopic(String prefix, TestInfo testInfo, int numberOfPartitions) {
        return createTopic(prefix, testInfo, null, numberOfPartitions);
    }

    public static String createTopic(String prefix, TestInfo testInfo, RepetitionInfo repetitionInfo, int numberOfPartitions) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (AdminClient ac = AdminClient.create(props)) {
            String topicName = String.format("%s-%s",
                    testName(testInfo, repetitionInfo), prefix);
            NewTopic topic = new NewTopic(topicName, numberOfPartitions, (short) 1);
            CreateTopicsResult result = ac.createTopics(Collections.singletonList(topic));
            result.all().get();
            return topicName;
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String createTopic(TestInfo testInfo) {
        return createTopic(testInfo, null);
    }

    public static String createTopic(TestInfo testInfo, RepetitionInfo repetitionInfo) {
        return createTopic("topic1", testInfo, repetitionInfo, 1);
    }

    public static String testName(TestInfo testInfo) {
        return testName(testInfo, null);
    }

    public static String testName(TestInfo testInfo, @Nullable RepetitionInfo repetitionInfo) {
        String testName = testInfo.getTestMethod()
                .orElseThrow(() -> new IllegalArgumentException("Test name not available"))
                .getName();
        if (repetitionInfo != null) {
            testName = testName + "-" + repetitionInfo.getCurrentRepetition();
        }
        return testName;
    }

    public static String expected(TestInfo testInfo) throws IOException, URISyntaxException {
        URL resource = TestUtils.class.getResource("/expected/" + testName(testInfo));
        if (resource == null) {
            throw new IllegalStateException("Resource missing for " + testName(testInfo));
        }
        return Files.lines(Paths.get(resource.toURI())).collect(Collectors.joining("\n"));
    }


    public static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.getValueInMS());
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @return all consumer groups existing in broker
     */
    public static List<String> getConsumerGroups(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient ac = AdminClient.create(props)) {
            return ac.listConsumerGroups().all().get().stream()
                    .map(ConsumerGroupListing::groupId).collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }

    }

    @SuppressWarnings("unused")
    public static String localDateTime(Instant instant) {
        return localDateTime(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()));
    }

    public static String localDateTime(LocalDateTime localDateTime) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
        return localDateTime.format(dateTimeFormatter);
    }

}
