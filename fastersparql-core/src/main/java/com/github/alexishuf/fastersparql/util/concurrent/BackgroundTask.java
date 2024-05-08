package com.github.alexishuf.fastersparql.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public interface BackgroundTask {
    default void sync() {
        CountDownLatch latch = new CountDownLatch(1);
        sync(latch);
        boolean interrupted = false;
        while (true) {
            try {
                if (latch.await(Long.MAX_VALUE, TimeUnit.NANOSECONDS))
                    break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Asynchronously calls {@code latch.countDown()} sometime after all sub-tasks that where
     * scheduled on this {@link BackgroundTask} <i>before</i> this call, have completed.
     *
     * @param latch a {@link CountDownLatch} to be notified
     */
    void sync(CountDownLatch latch);
}
