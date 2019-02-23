package kt.cli;

import joptsimple.OptionException;
import kt.cli.ConsumerOptionsBuilder.MissingTopicsException;
import kt.consumer.ConsumerOptions;
import kt.consumer.MultiConsumer;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static kt.cli.Console.printError;
import static kt.cli.Console.println;
import static kt.cli.ConsumerLimits.ConsumerLimitCalculationException;

public class Cli {

    private InfoPrinter infoPrinter;
    private RecordPrinter recordPrinter;
    private MultiConsumer consumer;
    private ConsumerOptionsBuilder consumerOptionsBuilder;

    Cli(InfoPrinter infoPrinter, RecordPrinter recordPrinter, MultiConsumer consumer,
        ConsumerOptionsBuilder consumerOptionsBuilder) {
        this.infoPrinter = infoPrinter;
        this.recordPrinter = recordPrinter;
        this.consumer = consumer;
        this.consumerOptionsBuilder = consumerOptionsBuilder;
    }

    /**
     * Parses arguments and starts consumer.
     *
     * @param args cli arguments
     * @return program exit code: 0 - ok, otherwise error
     */
    public int run(String[] args) {
        // Init and prepare options
        Console.initConsole();
        CliOptionsParser consoleOptionsParser = new CliOptionsParser();
        ConsumerOptions options;
        CliOptions cliOptions;
        try {
            cliOptions = consoleOptionsParser.parse(args);
            if (cliOptions.help) {
                consoleOptionsParser.printHelp();
                return 0;
            }

            if (cliOptions.version) {
                Map<String, Optional<String>> manifestAttributes = getJarAttributes(Cli.class, "Implementation-Title", "Implementation-Version");
                String version = String.format("%s %s",
                        manifestAttributes.get("Implementation-Title").orElse("n/a"),
                        manifestAttributes.get("Implementation-Version").orElse("n/a")
                );
                println(version);
                return 0;
            }

            options = consumerOptionsBuilder.buildConsumerOptions(cliOptions);
            recordPrinter.onTopicsUpdated(options.topics);
        } catch (OptionException | OptionValidationException | ConsumerLimitCalculationException e) {
            printError(e.getMessage());
            return 2;
        } catch (MissingTopicsException e) {
            printError("No topic found for given pattern.");
            return 3;
        }

        // Sart consumer
        consumer.start(options,
                consumerEvent -> infoPrinter.consumerEventReceived(consumerEvent, options, cliOptions),
                record -> recordPrinter.printRecords(record));


        return 0;
    }

    /**
     * @return jar manifest attributes.
     */
    public static Map<String, Optional<String>> getJarAttributes(Class<?> clazz, String... names) {
        String className = clazz.getSimpleName() + ".class";
        String classPath = clazz.getResource(className).toString();
        Map<String, Optional<String>> res = new HashMap<>();
        for (String name : names) {
            res.put(name, Optional.empty());
        }
        if (classPath.startsWith("jar")) {
            try {
                URL url = new URL(classPath);
                JarURLConnection jarConnection = (JarURLConnection) url.openConnection();
                Manifest manifest = jarConnection.getManifest();
                Attributes attributes = manifest.getMainAttributes();
                for (String name : names) {
                    String value = attributes.getValue(name);
                    if (value != null) {
                        res.put(name, Optional.of(value));
                    }
                }

            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return res;
    }

}
