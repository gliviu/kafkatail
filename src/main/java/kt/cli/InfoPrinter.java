package kt.cli;

import kt.consumer.ConsumerEvent;
import kt.consumer.ConsumerOptions;

import javax.annotation.Nullable;

import static kt.cli.Console.*;
import static kt.cli.Dates.localDateTime;

public class InfoPrinter {
    public void cliEventReceived(CliEvent cliEvent, @Nullable String eventInfo, ConsumerOptions consumerOptions, CliOptions cliOptions) {
        switch (cliEvent) {
            case WARNING_OCCURRED:
                println();
                moveCursorUp();
                eraseLine();
                println(warn(eventInfo));
                break;
            case GET_ALL_TOPICS:
                print(".");
                break;
            case GET_ALL_TOPICS_END:
                print(".");
                break;
            case START_CONSUME:
                println();
                moveCursorUp();
                eraseLine();
                printInfo(consumerOptions, cliOptions);
                break;
            case REACHED_END_CONSUMER_LIMIT:
                println(warn("Reached consumer start limit " + localDateTime(consumerOptions.endConsumerLimit)));
                break;
            case END_CONSUME:
                // ignore
                break;
            case ASSIGN_PARTITIONS:
            case GET_PARTITIONS:
            case SEEK_BACK:
            case SEEK_TO_END:
            case SEEK_TO_BEGINNING:
                print(".");
                break;
            default:
                throw new IllegalArgumentException("Unexpected event " + cliEvent);
        }
    }

    public void cliEventReceived(CliEvent cliEvent, ConsumerOptions consumerOptions, CliOptions cliOptions) {
        cliEventReceived(cliEvent, null, consumerOptions, cliOptions);
    }

    public void consumerEventReceived(ConsumerEvent consumerEvent, ConsumerOptions options, CliOptions cliOptions) {
        cliEventReceived(CliEvent.valueOf(consumerEvent.name()), options, cliOptions);
    }

    public void printInfo(ConsumerOptions options, CliOptions cliOptions) {
        println("Server: " + options.broker);
        if (options.startConsumerLimit != null) {
            println("Consume from: " + localDateTime(options.startConsumerLimit));
        }
        if (options.fromBeginning) {
            println("Consume from: beginning");
        }
        if (options.endConsumerLimit != null) {
            println("Consume until: " + localDateTime(options.endConsumerLimit));
        }
        if (cliOptions.shouldConsumeAllTopics()) {
            print(warn("Consuming from all topics"));
            if (!cliOptions.excludeTopics.isEmpty()) {
                print(warn(" excluding " + String.join(", ", cliOptions.excludeTopics)));
            }
            println();
        }
        println("Topics: " + String.join(" ", options.topics));
        println();
    }


}
