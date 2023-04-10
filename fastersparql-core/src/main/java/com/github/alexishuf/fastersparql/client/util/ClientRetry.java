package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ClientRetry {
    private static final Logger log = LoggerFactory.getLogger(ClientRetry.class);

    public static boolean retry(int retryNumber, Throwable error, Runnable onRetry) {
        boolean retry = retryNumber <= FSProperties.maxRetries()
                           && error instanceof FSServerException e && e.shouldRetry();
        if (retry) {
            Thread.startVirtualThread(() -> {
                try {
                    long ms = FSProperties.retryWait(TimeUnit.MILLISECONDS);
                    log.debug("waiting {} before retry after {} failed attempts", ms, retryNumber);
                    Thread.sleep(ms);
                } catch (InterruptedException ignored) {}
                try {
                    onRetry.run();
                } catch (Throwable t) {
                    retry(retryNumber+1, t, onRetry);
                }
            });
        }
        return retry;
    }
}
