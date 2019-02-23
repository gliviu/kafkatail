package kt.cli;

import kt.consumer.MultiConsumer;

public class Main {
    public static void main(String[] args) {
        MultiConsumer consumer = new MultiConsumer();
        ShutdownHook shutdownHook = new ShutdownHook(consumer);
        InfoPrinter infoPrinter = new InfoPrinter();
        KafkaAdmin kafkaAdmin = new KafkaAdmin();
        ConsumerOptionsBuilder consumerOptionsBuilder = new ConsumerOptionsBuilder(infoPrinter, kafkaAdmin);

        Cli cli = new Cli(infoPrinter, new RecordPrinter(), consumer, consumerOptionsBuilder);
        int exitCode = 1;
        try {
            exitCode = cli.run(args);
        } catch(Exception e) {
            e.printStackTrace();
        }

        shutdownHook.cleanupCompleted();
        Runtime.getRuntime().halt(exitCode);
    }
}
