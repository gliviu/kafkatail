package kt.cli;

import kt.consumer.ConsumerEvent;
import kt.consumer.ConsumerOptions;
import kt.markers.InternalState;

import javax.annotation.Nullable;

import static kt.cli.Console.*;
import static kt.cli.Dates.localDateTime;

class InfoPrinter {
    @InternalState
    private boolean consumingNewRecords;

    @InternalState
    private int nextAwakeCharacter;

    private char[] awakeCharacters = new char[]{'\\', '|', '/', '-'};


    void cliEventReceived(CliEvent cliEvent, @Nullable String eventInfo, ConsumerOptions consumerOptions, CliOptions cliOptions) {
        switch (cliEvent) {
            case WARNING_OCCURRED:
                println(warn(eventInfo));
                break;
            case GET_ALL_TOPICS:
                awake();
                break;
            case GET_ALL_TOPICS_END:
                awake();
                break;
            case START_CONSUME:
                awake();
                printInfo(consumerOptions, cliOptions);
                break;
            case NO_HISTORICAL_RECORDS_AVAILABLE:
                println(warn("No historical records available"));
                break;
            case REACHED_END_CONSUMER_LIMIT:
                println(warn("Reached consumer end limit " + localDateTime(consumerOptions.endConsumerLimit)));
                break;
            case CONSUMING_NEW_RECORDS:
                println(warn("Consuming new records"));
                consumingNewRecords = true;
                break;
            case END_CONSUME:
                // ignore
                break;
            case START_ORDERING_RECORDS:
            case END_ORDERING_RECORDS:
                // ignore
                break;
            case POLL_RECORDS:
                if(consumerOptions.shouldReadHistoricalRecords() && !consumingNewRecords) {
                    awake();
                }
                break;
            case ASSIGN_PARTITIONS:
            case GET_PARTITIONS:
            case SEEK_BACK:
            case SEEK_TO_END:
            case SEEK_TO_BEGINNING:
                awake();
                break;
            default:
                throw new IllegalArgumentException("Unexpected event " + cliEvent);
        }
    }

    private void awake() {
        print(awakeCharacters[nextAwakeCharacter]);
        nextAwakeCharacter++;
        nextAwakeCharacter = nextAwakeCharacter % awakeCharacters.length;
        moveCursorStart();
    }

    void cliEventReceived(CliEvent cliEvent, ConsumerOptions consumerOptions, CliOptions cliOptions) {
        cliEventReceived(cliEvent, null, consumerOptions, cliOptions);
    }

    void consumerEventReceived(ConsumerEvent consumerEvent, ConsumerOptions options, CliOptions cliOptions) {
        cliEventReceived(CliEvent.valueOf(consumerEvent.name()), options, cliOptions);
    }

    private void printInfo(ConsumerOptions options, CliOptions cliOptions) {
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
        if (cliOptions.realTime) {
            println(warn("Realtime: true"));
        }
        println("Topics: " + String.join(" ", options.topics));
        println();
    }


}
