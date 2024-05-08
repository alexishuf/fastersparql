package com.github.alexishuf.fastersparql.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.System.arraycopy;

public class BackgroundTasks {
    private static final VarHandle REGISTER_LOCK;
    static {
        try {
            REGISTER_LOCK = MethodHandles.lookup().findStaticVarHandle(BackgroundTasks.class, "plainRegisterLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static BackgroundTask[] tasks = new BackgroundTask[10];
    private static int tasksSize;
    @SuppressWarnings("unused") private static int plainRegisterLock;

    public static void register(BackgroundTask task) {
        while ((int)REGISTER_LOCK.compareAndExchangeAcquire(0, 1) != 0) Thread.onSpinWait();
        try {
            if (tasksSize >= tasks.length)
                tasks = Arrays.copyOf(tasks, tasks.length<<1);
            tasks[tasksSize++] = task;
        } finally {
            REGISTER_LOCK.setRelease(0);
        }
    }

    @SuppressWarnings("unused") public static void unregister(BackgroundTask task) {
        while ((int)REGISTER_LOCK.compareAndExchangeAcquire(0, 1) != 0) Thread.onSpinWait();
        try {
            for (int i = 0; i < tasksSize; i++) {
                if (tasks[i] == task) {
                    arraycopy(tasks, i+1, tasks, i, tasksSize-(i+1));
                    i = --tasksSize;
                }
            }
        } finally {
            REGISTER_LOCK.setRelease(0);
        }
    }

    public static void sync() {
        boolean interrupted = false;
        while (!sync(Long.MAX_VALUE, TimeUnit.NANOSECONDS))
            interrupted |= Thread.interrupted(); // clears interrupt flag
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    public static boolean sync(long timeout, TimeUnit timeoutUnit) {
        var latch = new CountDownLatch(tasksSize);
        for (int i = 0; i < tasksSize; i++)
            tasks[i].sync(latch);
        try {
            return latch.await(timeout, timeoutUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
