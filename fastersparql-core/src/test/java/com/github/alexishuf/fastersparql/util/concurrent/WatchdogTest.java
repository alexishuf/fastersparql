package com.github.alexishuf.fastersparql.util.concurrent;

import org.junit.jupiter.api.RepeatedTest;

import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.nanoTime;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WatchdogTest {
    private void sleep(int nanos) {
        for (long start = nanoTime(); nanoTime()-start < nanos; )
            Thread.yield();
    }

    @RepeatedTest(20) void test() {
        int longNanos = 10_000_000;
        AtomicInteger triggers = new AtomicInteger(0);
        try (Watchdog w = new Watchdog(triggers::getAndIncrement)) {
            sleep(longNanos);
            assertEquals(0, triggers.get());

            w.start(100_00);
            sleep(longNanos);
            assertEquals(1, triggers.get());

            // every start nullifies the previous untriggered start
            for (long start = nanoTime(); nanoTime()-start < 2_000_000; )
                w.start(2_000_000);
            // only last start() is effective, despite first ones nearing their deadline
            assertEquals(1, triggers.get());
            sleep(100_000);
            assertEquals(1, triggers.get());

            // sleep a bit for the last start(). Due to the flooding,
            // last parkNanos() might exceed its nanos parameter
            sleep(longNanos);
            assertEquals(2, triggers.get());

            // wait even more to check no more triggers happen
            sleep(longNanos);
            assertEquals(2, triggers.get()); // no more triggers

            // test if watchdog can trigger again after flood
            w.start(100_000);
            sleep(longNanos);
            assertEquals(3, triggers.get());

            // race close() and trigger
            w.start(500_000);
        }
        sleep(10_000_000);
        assertEquals(3, triggers.get()); // close() cancelled last start()
    }

    @RepeatedTest(10) void testCancel() {
        var triggered = new AtomicInteger(0);
        try (var w = new Watchdog(triggered::incrementAndGet)) {
            w.start(500_000);
            w.stop();

            sleep(10_000_000);
            assertEquals(0, triggered.get());

            w.start(500_000);
            sleep(10_000_000);
            assertEquals(1, triggered.get());
        }
    }

}