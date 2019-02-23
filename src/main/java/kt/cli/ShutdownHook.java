package kt.cli;

import kt.consumer.MultiConsumer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ShutdownHook {
    private static final int WAIT_FOR_CLEANUP_TIMEOUT_SECONDS = 2;
    private final CountDownLatch cleanupCompleted = new CountDownLatch(1);

    ShutdownHook(MultiConsumer consumer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.stop();
            try {
                cleanupCompleted.await(WAIT_FOR_CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        }));
    }

    /**
     * Call this when app finished all cleanup and is ready to close.
     */
    public void cleanupCompleted() {
        cleanupCompleted.countDown();
    }

}
