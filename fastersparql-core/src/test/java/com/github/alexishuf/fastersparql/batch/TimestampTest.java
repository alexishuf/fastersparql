package com.github.alexishuf.fastersparql.batch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TimestampTest {

    @Test void testNoWait() {
        for (int i = 0; i < 100_000; i++) {
            long ts0 = Timestamp.nanoTime();
            long ns0 = System.nanoTime();
            long ts1 = Timestamp.nanoTime();
            long ns1 = System.nanoTime();

            assertTrue(ns0 > ts0, "Timestamp.nanoTime() ahead of System.nanoTime()");
            assertTrue(ns1 > ts1, "Timestamp.nanoTime() ahead of System.nanoTime()");
            assertTrue(ts1 >= ts0, "non-monotonic");
        }
    }

    @Test
    void testSleep() {
        for (int i = 0; i < 500; i++) {
            long ts0 = Timestamp.nanoTime();
            long ns0 = System.nanoTime();
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignored) {}
            long ts1 = Timestamp.nanoTime();
            long ns1 = System.nanoTime();

            assertTrue(ns1 > ns0, "non-monotonic");
            assertTrue(ts1 > ts0, "non-monotonic");
            assertTrue(ts0 < ns0, "Timestamp.nanoTime() ahead of System.nanoTime()");
            assertTrue(ts1 < ns1, "Timestamp.nanoTime() ahead of System.nanoTime()");
            long err = (ns1-ns0) - (ts1-ts0);
            assertTrue(err < 1_000_000);
        }
    }


}