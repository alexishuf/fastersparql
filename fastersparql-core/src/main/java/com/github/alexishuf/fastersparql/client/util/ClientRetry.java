package com.github.alexishuf.fastersparql.client.util;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ClientRetry {
    public static boolean retry(int retryNumber, Throwable error, Runnable onRetry,
                                Consumer<Throwable> onDefinitiveFailure) {
        if (retryNumber > FSProperties.maxRetries()) {
            onDefinitiveFailure.accept(error);
            return false;
        } else {
            Thread.ofVirtual().start(() -> {
                try {
                    Thread.sleep(FSProperties.retryWait(TimeUnit.MILLISECONDS));
                } catch (InterruptedException ignored) {}
                try {
                    onRetry.run();
                } catch (Throwable t) {
                    retry(retryNumber+1, t, onRetry, onDefinitiveFailure);
                }
            });
            return true;
        }
    }
}
