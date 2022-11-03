package com.github.alexishuf.fastersparql.client.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class VThreadTaskSet implements AutoCloseable, Consumer<Future<?>> {
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicInteger nextTaskId = new AtomicInteger(1);
    private final String name;
    private final List<Future<?>> tasks = Collections.synchronizedList(new ArrayList<>());

    public VThreadTaskSet(String name) {
        this.name = name;
    }

    public static void repeatAndWait(String name, int count, Function<Integer, ?> runnable)  throws Exception {
        try (var set = new VThreadTaskSet(name)) { set.repeat(count, runnable); }
    }

    public static void repeatAndWait(String name, int count, Runnable runnable) throws Exception {
        try (var set = new VThreadTaskSet(name)) { set.repeat(count, runnable); }
    }

    public static void repeatAndWait(String name, int count, Callable<?> runnable) throws Exception {
        try (var set = new VThreadTaskSet(name)) { set.repeat(count, runnable); }
    }

    public static void repeatAndWait(String name, int count, Consumer<Integer> runnable)
            throws Exception {
        try (var set = new VThreadTaskSet(name)) { set.repeat(count, runnable); }
    }

    public void add(Runnable runnable) {
        add(executor.submit(() -> {
            String oldName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(this+"-" + nextTaskId.getAndIncrement());
                runnable.run();
            } finally {
                Thread.currentThread().setName(oldName);
            }
        }));
    }

    public void add(Callable<?> callable) {
        add(executor.submit(() -> {
            String oldName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(this+"-"+nextTaskId.getAndIncrement());
                return callable.call();
            } finally {
                Thread.currentThread().setName(oldName);
            }
        }));
    }

    public void repeat(int times, Function<Integer, ?> runnable) {
        for (int i = 0; i < times; i++) {
            int number = i;
            add(() -> runnable.apply(number));
        }
    }

    public void repeat(int times, Consumer<Integer> runnable) {
        for (int i = 0; i < times; i++) {
            int number = i;
            add(() -> runnable.accept(number));
        }
    }

    public void repeat(int times, Runnable runnable) {
        for (int i = 0; i < times; i++) add(runnable);
    }

    public void repeat(int times, Callable<?> callable) {
        for (int i = 0; i < times; i++) add(callable);
    }

    @Override public void close() throws Exception {
        executor.close();
        Throwable error = null;
        for (Future<?> task : tasks) {
            try {
                task.get();
            } catch (Throwable t) {
                Throwable cause = t instanceof ExecutionException ? t.getCause() : t;
                if (error == null) error = cause;
                else if (error.getSuppressed().length < 2) error.addSuppressed(cause);
            }
        }
        if      (error instanceof Error e)            throw e;
        else if (error instanceof RuntimeException e) throw e;
        else if (error instanceof Exception e)        throw e;
    }

    public           void       add(Future<?> task) { tasks.add(task); }

    @Override public void    accept(Future<?> task) { tasks.add(task); }

    @Override public String toString() {
        return name;
    }
}

