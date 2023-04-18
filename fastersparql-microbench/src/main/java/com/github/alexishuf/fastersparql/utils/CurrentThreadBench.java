package com.github.alexishuf.fastersparql.utils;


import org.openjdk.jmh.annotations.*;

import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.lang.invoke.MethodHandles.lookup;

@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 4, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1, warmups = 0, jvmArgsAppend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class CurrentThreadBench {
    private static final VarHandle COUNTER;
    static {
        try {
            COUNTER = lookup().findVarHandle(CurrentThreadBench.class, "counter", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private int counter;
    private boolean stop;
    private Thread thread, nil;

    @Setup public void setup() {
        counter = 0;
        nil = Thread.startVirtualThread(() -> COUNTER.getAndAdd((CurrentThreadBench.this), 1));
        thread = Thread.startVirtualThread(() -> {
            LockSupport.park();
            nil = null;
            while (!stop)
                COUNTER.getAndAdd(CurrentThreadBench.this, 1);
        });
    }

    @TearDown public void tearDown() {
        stop = true;
        try {
            thread.join(100);
        } catch (InterruptedException ignored) {}
    }

    @Benchmark public Thread currentThread() { return Thread.currentThread(); }

    @Benchmark public Thread unparkNotParked() {
        Thread t = thread;
        LockSupport.unpark(t);
        return t;
    }

    @Benchmark public Thread unparkNull() {
        Thread t = nil;
        LockSupport.unpark(t);
        return t;
    }
}