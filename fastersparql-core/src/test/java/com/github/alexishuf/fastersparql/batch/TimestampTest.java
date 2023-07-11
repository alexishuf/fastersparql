package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.util.concurrent.Async;
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
        long errSum = 0;
        int rounds = 100;
        int ms = 50;
        for (int i = -4; i < rounds; i++) {
            if (i == 0) errSum = 0;
            long ts0 = Timestamp.nanoTime();
            long ns00 = System.nanoTime();
            long ns0 = System.nanoTime();
            long ns01 = System.nanoTime();
            Async.uninterruptibleSleep(ms);
            long ts1 = Timestamp.nanoTime();
            long ns1 = System.nanoTime();

            assertTrue(ns1 > ns0, "non-monotonic");
            assertTrue(ts1 > ts0, "non-monotonic");
            errSum += (ns1-ns0 + 2*(ns01-ns00)) - (ts1-ts0);
        }
        double avgErrMs = Math.abs(errSum/(double)rounds/1_000_000L);
        double maxErrMs = ms*0.10;
        System.out.println("avgErr="+avgErrMs+", maxErrMs="+maxErrMs);
        assertTrue(avgErrMs < maxErrMs, "avgErr="+avgErrMs+", maxErrMs="+maxErrMs);
    }


}