package kt.cli;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.HashSet;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static kt.cli.Console.*;

class CliOptionsParser {
    OptionParser parser;

    public CliOptions parse(String... args) {
        parser = new OptionParser();
        OptionSpec<String> brokerSpec = parser.accepts("b")
                .withRequiredArg().ofType(String.class);
        OptionSpec<String> excludeTopicsSpec = parser.accepts("x")
                .withRequiredArg().ofType(String.class).withValuesSeparatedBy(",");
        OptionSpec<String> startConsumerLimitSpec = parser.acceptsAll(asList("s", "since"))
                .withRequiredArg().ofType(String.class);
        OptionSpec<String> consumeDurationSpec = parser.acceptsAll(asList("a", "amount"))
                .withRequiredArg().ofType(String.class);
        OptionSpec<String> linesSpec = parser.acceptsAll(asList("n", "lines"))
                .withRequiredArg().ofType(String.class);
        OptionSpec<String> endConsumerLimitSpec = parser.acceptsAll(asList("u", "until"))
                .withRequiredArg().ofType(String.class);
        OptionSpec<Void> helpSpec = parser.acceptsAll(asList("h", "help"));
        OptionSpec<Void> versionSpec = parser.acceptsAll(asList("v", "version"));
        OptionSet options = parser.parse(args);

        CliOptions cliOptions = new CliOptions();
        cliOptions.topicPatterns = options.nonOptionArguments().stream().map(Object::toString).collect(Collectors.toSet());
        cliOptions.broker = options.valueOf(brokerSpec);
        cliOptions.excludeTopics = new HashSet<>(options.valuesOf(excludeTopicsSpec));
        cliOptions.startConsumerLimit = options.valueOf(startConsumerLimitSpec);
        cliOptions.consumeDuration = options.valueOf(consumeDurationSpec);
        cliOptions.endConsumerLimit = options.valueOf(endConsumerLimitSpec);
        cliOptions.lines = options.valueOf(linesSpec);
        cliOptions.help = options.has(helpSpec);
        cliOptions.version = options.has(versionSpec);
        return cliOptions;
    }

    public void printHelp() {
        print(String.format("" +
                bold("Usage:") + " kafkatail [options] [--] [topic-pattern1 topic-pattern2 ...]             %n" +
                "%n" +
                "Tails multiple kafka topics and prints records to standard output as they arrive.%n" +
                "With no options, consumes new records from all topics at localhost:9092.%n" +
                "Default display format is 'partition offset timestamp topic [key] value'.%n" +
                "Timestamp is marked as N/A if missing from record.%n" +
                "%n" +
                bold("Options%n") +
                "-b host[:port]      Broker address (default: localhost:9092)%n" +
                "-x exclude-patterns Topics to be excluded%n" +
                "-h --help           Show help%n" +
                "-v --version        Show version%n" +
                "-s --since date     Print previous records starting from date%n" +
                "-u --until date     Consume records until date is reached%n" +
                "-a --amount amount  Consume certain amount of records starting with -s, then stop%n" +
                warn("-s -u -a are experimental and might not work as expected%n") +
                "%n" +
                bold("Topic selection%n") +
                "Topic names are matched by substring.%n" +
                green("kafkatail") + "                        tail all topics at localhost:9092%n" +
                green("kafkatail emp dep") + "                tail topics containing emp and dep in their name%n" +
                green("kafkatail -x emp,ord") + "             tail all topics except those containing emp, ord%n" +
                green("kafkatail -x hist,temp -- dep") + "    tail all topics containing dep, excluding hist, temp%n" +
                "%n" +
                bold("Offset range%n") +
                "Starting kafkatail with no options will print only new records.%n" +
                "It is possible to show previous records by using -s -u and -a.%n" +
                green("kafkatail -s 10m                                   ") + "tail starting from 10 minutes ago      %n" +
                green("kafkatail -s 1d -u 2h topic1 topic2                ") + "tail since 1 day ago until 2 hours ago %n" +
                green("kafkatail -s 2015-01-01T10:00 -u 2015-01-01T11:00  ") + "tail between specified interval        %n" +
                green("kafkatail -s 10:00 -u 11:00                        ") + "tail between hours 10..11 today        %n" +
                green("kafkatail -s 10:00 -a 30m                          ") + "tail between 10:00..10:30 today        %n" +
                "Formats accepted by -s and -u:%n" +
                "* absolute local date/time - 2015-01-01T18:00, 2015-01-01T18:00:33%n" +
                "* today local time - 18:00, 18:00:33%n" +
                "* relative past time - 10m (ten minutes ago)%n" +
                "  accepted units: day d, hour h, minute m, second s%n" +
                "Amount (-a) format: 10m (ten minutes).%n" +
                "Options -u and -a cannot be used together.%n" +
                "%n"));
    }

}