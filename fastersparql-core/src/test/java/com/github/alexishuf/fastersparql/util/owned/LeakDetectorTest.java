package com.github.alexishuf.fastersparql.util.owned;

import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.BackgroundTasks;
import com.github.alexishuf.fastersparql.util.owned.LeakDetector.LeakState;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jctools.queues.atomic.MpscUnboundedAtomicArrayQueue;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.util.concurrent.Timestamp.nanoTime;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeakDetectorTest {
    private static final Logger log = LoggerFactory.getLogger(LeakDetectorTest.class);
    private static final AtomicInteger instances = new AtomicInteger();
    private static final AtomicInteger leaks     = new AtomicInteger();

    public static class O extends AbstractOwned<O> {
        public int id;
        public static Orphan<O> create() { return new Concrete(); }

        private O() {id = instances.getAndIncrement();}

        private static final class Concrete extends O implements Orphan<O> {
            @Override public O takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override protected LeakState makeLeakState(@Nullable OwnershipHistory history) {
            return new OLeakState(this, history);
        }

        private static final class OLeakState extends LeakState {
            public OLeakState(Owned<?> o, @Nullable OwnershipHistory h) {super(o, h);}
            @Override protected void singleThreadPrintLeak(PrintStream out) {
                leaks.getAndIncrement();
            }
        }

        @Override public @Nullable O recycle(Object currentOwner) {
            internalMarkRecycled(currentOwner);
            return null;
        }
    }

    @Test void test() {
        if (!LeakDetector.ENABLED) {
            log.warn("Will not test LeakDetector, disabled");
            return;
        }
        long durationNs = Duration.ofSeconds(1).toNanos();
        log.info("Spamming object creation for {} seconds", NANOSECONDS.toMillis(durationNs)/1000.0);
        try (var confuser = new Confuser()) {
            // create lots of objects
            for (long end = nanoTime()+durationNs; nanoTime() < end; ) {
                for (int i = 0; i < 512; i++) {
                    O o = O.create().takeOwnership(this);
                    if ((o.id & 0xf) != 0)
                        confuser.eden.offer(o);
                }
            }
        } // stops confuser thread
        log.info("Doing 4 System.gc() calls with 250ms pauses");
        for (int i = 0; i < 4; i++) {
            System.gc();
            Async.uninterruptibleSleep(250);
            BackgroundTasks.sync();
        }

        int total = instances.get();
        int recycled = 128 + (total>>8);
        int leaked = total - recycled;
        int expected = leaked - leaked/3;
        int detected = leaks.get();
        log.info("{} instances, max {} recycled, {} leaks detected", total, recycled, detected);
        assertTrue(detected > expected, format("detected %,d leaks out of %,d", detected, leaked));
    }

    private class Confuser extends Thread implements AutoCloseable {
        private static final int MASK = 127;
        private final MpscUnboundedAtomicArrayQueue<O> eden, delayed;
        private final O[] working = new O[MASK +1];
        private volatile boolean stop;

        public Confuser() {
            super("Confuser");
            eden    = new MpscUnboundedAtomicArrayQueue<>(1024);
            delayed = new MpscUnboundedAtomicArrayQueue<>(256);
            setDaemon(true);
            start();
        }

        @Override public void close() {
            stop = true;
            eden.drain(ignored -> {});
            delayed.drain(ignored -> {});
            Async.uninterruptibleJoin(this);
            for (int i = 0; i < working.length; i++)
                working[i] = Owned.recycle(working[i], this);
        }

        @Override public void run() {
            var test = LeakDetectorTest.this;
            for (int i = 0; !stop; ++i) {
                O o = eden.poll();
                if (o != null) {
                    o.transferOwnership(test, this);
                    if ((o.id&0xff) == 0xff)
                        o.recycle(this);
                    else if ((o.id&0x7) == 0x7)
                        working[o.id&MASK] = o;
                    else
                        delayed.offer(o);
                }
                if ((i&0x7f) == 0x7f)
                    delayed.drain(s -> working[s.id&MASK] = s);
            }

        }
    }



}