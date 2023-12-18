package com.github.alexishuf.fastersparql.util.concurrent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

class UnparkerTest {

    @RepeatedTest(4)
    void test() {
        var semaphore = new Semaphore(0);
        for (int round = 0; round < 1_000; round++) {
            Thread parked = new Thread(() -> {
                LockSupport.park();
                semaphore.release();
            }, "parker-"+round);
            parked.start();
            Unparker.unpark(parked);
            assertTimeout(ofSeconds(1), () -> semaphore.acquireUninterruptibly());
            assertEquals(0, semaphore.availablePermits());
        }
        Async.uninterruptibleSleep(200);
    }

    private static final class ParkerThread extends Thread {
        private static final VarHandle CAN_PARK;
        static {
            try {
                CAN_PARK = MethodHandles.lookup().findVarHandle(ParkerThread.class, "plainCanPark", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        private final Semaphore hasUnparked = new Semaphore(0);
        @SuppressWarnings("unused") private int plainCanPark;

        public ParkerThread(int id) {
            super("Parker["+id+"]");
            start();
        }

        @Override public String toString() {return getName();}

        public void shutdown() {
            interrupt();
            CAN_PARK.setRelease(this, 1);
            LockSupport.unpark(this);
        }

        public void park() {
            hasUnparked.drainPermits();
            CAN_PARK.setRelease(this, 1);
        }

        public void assertUnparked() {
            try {
                assertTrue(hasUnparked.tryAcquire(2, TimeUnit.SECONDS),
                           "did not receive unpark");
            } catch (InterruptedException e) {
                Assertions.fail(e);
            }
        }

        @Override public void run() {
            while (!interrupted()) {
                while ((int)CAN_PARK.compareAndExchangeAcquire(this, 1, 0) != 1 && interrupted())
                    Thread.yield();
                if (interrupted())
                    break;
                LockSupport.park();
                hasUnparked.release();
            }
        }
    }

    private static final class UnparkerThread extends Thread {
        private final Semaphore canUnpark = new Semaphore(0);
        private final ParkerThread[] parkers;
        private final int parkersBegin, parkersEnd;

        public UnparkerThread(int id, ParkerThread[] parkers, int parkersBegin, int parkersEnd) {
            super("Unparker["+id+"]");
            this.parkers      = parkers;
            this.parkersBegin = parkersBegin;
            this.parkersEnd   = parkersEnd;
            assert parkersBegin >= 0 && parkersEnd <= parkers.length;
            start();
        }

        @Override public String toString() { return getName(); }

        public void unpark() {
            canUnpark.release();
        }

        public void shutdown() {
            interrupt();
            canUnpark.release();
        }

        @Override public void run() {
            try {
                while (!interrupted()) {
                    canUnpark.acquire();
                    for (int i = parkersBegin; i < parkersEnd; i++)
                        Unparker.unpark(parkers[i]);
                }
            } catch (InterruptedException ignored) {}
        }
    }

    @ParameterizedTest @ValueSource(ints = {2, 4, 8, 16, 32, 64, 128, 256})
    void concurrentTest(int nParkers) {
        try {
            int threads = Runtime.getRuntime().availableProcessors();
            int chunk = nParkers / threads;
            ParkerThread[] parkers = new ParkerThread[nParkers];
            UnparkerThread[] unparkers = new UnparkerThread[threads];
            for (int i = 0; i < nParkers; i++)
                parkers[i] = new ParkerThread(i);
            for (int i = 0; i < threads; i++) {
                int begin = i * chunk, end = i == threads - 1 ? nParkers : begin + chunk;
                unparkers[i] = new UnparkerThread(i, parkers, begin, end);
            }

            for (int round = 0; round < 5_000; round++) {
                for (var t :   parkers) t.  park();
                for (var t : unparkers) t.unpark();
                for (var t :   parkers) t.assertUnparked();
            }
            for (var t :   parkers) t.shutdown();
            for (var t : unparkers) t.shutdown();
        } catch (Throwable t) {
            ThreadJournal.dumpAndReset(System.out, 40);
            throw t;
        }
    }

}