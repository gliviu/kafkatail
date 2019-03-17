package kt.cli;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;
import java.util.Comparator;
import java.util.Set;

import static kt.cli.Console.println;
import static kt.cli.Console.warn;
import static kt.cli.Dates.localDateTime;

class RecordPrinter {
    private final static int MAX_KEY_PAD = 15;
    private final static int MAX_TIMESTAMP_PAD = localDateTime(Instant.now()).length();
    private final static int MAX_TOPIC_PAD = 15;
    private int maxKeyDisplaySize = 0;
    private int maxTopicDisplaySize = 0;
    private int maxOffsetDisplaySize = 0;

    void onTopicsUpdated(Set<String> topics) {
        maxTopicDisplaySize = topics.stream()
                .max(Comparator.comparing(String::length))
                .map(String::length)
                .map(maxLen -> maxLen > MAX_TOPIC_PAD ? MAX_TOPIC_PAD : maxLen)
                .get();
    }

    void printRecords(ConsumerRecord<String, String> record) {
        String topicName = Console.yellow(padRight(record.topic(), maxTopicDisplaySize));

        String key = record.key() == null ? "" : record.key() + " ";
        if (maxKeyDisplaySize < key.length() && key.length() < MAX_KEY_PAD) {
            maxKeyDisplaySize = key.length();
        }
        key = padRight(key, maxKeyDisplaySize);

        String offset = Long.toString(record.offset());
        if(maxOffsetDisplaySize<offset.length()) {
            maxOffsetDisplaySize = offset.length();
        }

        String localDateTime = padRight(timestamp(record.timestamp()), MAX_TIMESTAMP_PAD);
        println(String.format("%d %s %s %s %s%s", record.partition(), offset, localDateTime, topicName, key, record.value()));
    }

    private String padRight(String s, int n) {
        return n == 0 ? s : String.format("%-" + n + "s", s);
    }

    private String timestamp(long kafkaTimestamp) {
        return kafkaTimestamp == -1 ? warn("N/A") : localDateTime(Instant.ofEpochMilli(kafkaTimestamp));
    }


}
