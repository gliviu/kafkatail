package kt.manual;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class FileLogger {

    private static final AtomicReference<String> logFilePath = new AtomicReference<>();

    private static final int MAX_BUFFER_SIZE = 100000; // synk to file each time this is limit reached or when program shuts down.
    private static final Duration SYNK_RATE_SECONDS = Duration.ofSeconds(5);

    private static final ConcurrentLinkedQueue<String> buffer = new ConcurrentLinkedQueue<>();
    private static final AtomicBoolean synkRunning = new AtomicBoolean(false);

    static {
        startSyncScheduler();
        Runtime.getRuntime().addShutdownHook(new Thread(FileLogger::synk));
    }

    static void changeLogName() {
        if(logFilePath.get()!=null) {
            synk();
        }
        logFilePath.set(String.format("%s/kafkatail-%s.log",
                System.getProperty("java.io.tmpdir"), Instant.now().toString().replace(":", "-")));
    }

    static String getlogPath() {
        return logFilePath.get();
    }

    static void write(String format, Object... args) {
        buffer.offer(LocalDateTime.now().toString() + " - " + String.format(format, args) + "\n");
        if (buffer.size() > MAX_BUFFER_SIZE && !synkRunning.get()) {
            CompletableFuture.runAsync(FileLogger::synk);
        }
    }

    synchronized private static void synk() {
        synkRunning.set(true);
        try (FileWriter fr = new FileWriter(logFilePath.get(), true)) {
            while (!buffer.isEmpty()) {
                fr.write(buffer.poll());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            synkRunning.set(false);
        }

    }

    private static void startSyncScheduler() {
        Thread synkScheduler = new Thread(() -> {
            while(true){
                delay();
                synk();
            }

        });
        synkScheduler.setDaemon(true);
        synkScheduler.start();
    }

    private static void delay() {
        try {
            Thread.sleep(SYNK_RATE_SECONDS.toMillis());
        } catch (InterruptedException e) {
            // ignore
        }
    }
}