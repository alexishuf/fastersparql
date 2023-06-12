package com.github.alexishuf.fastersparql.util;

import com.github.alexishuf.fastersparql.util.concurrent.Async;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public final class ExceptionCondenser<T extends Throwable> {
    private T acc;
    private final Class<T> accClass;
    private final Function<Throwable, T> factory;

    public ExceptionCondenser(Class<T> accClass, Function<Throwable, T> factory) {
        this.accClass = accClass;
        this.factory = factory;
    }

    public static ExceptionCondenser<RuntimeException> runtimeExceptionCondenser() {
        return new ExceptionCondenser<>(RuntimeException.class, RuntimeException::new);
    }

    public static void throwAsUnchecked(Throwable throwable) {
        if      (throwable instanceof Error e)            throw e;
        else if (throwable instanceof RuntimeException e) throw e;
        else if (throwable != null)                       throw new RuntimeException(throwable);
    }

    public void condense(@Nullable Throwable t) {
        if (t != null) {
            if (acc == null) //noinspection unchecked
                acc = accClass.isInstance(t) ? (T) t : factory.apply(t);
            else acc.addSuppressed(t);
        }
    }

    public <V> CompletionStage<V> condense(V value, CompletionStage<?> faulty) {
        var future = new CompletableFuture<V>();
        faulty.whenComplete((ignored, err) -> {
            condense(err);
            if (acc == null) future.complete(value);
            else             future.completeExceptionally(acc);
        });
        return future;
    }

    public @Nullable T get() { return acc; }

    public void throwIf() throws T {
        if (acc != null) throw acc;
    }

    public <V> boolean complete(CompletableFuture<V> future, @Nullable V value) {
        return acc == null ? future.complete(value) : future.completeExceptionally(acc);
    }

    public static void closeAll(Collection<? extends AutoCloseable> list) {
        closeAll(list.iterator());
    }
    public static void closeAll(Iterator<? extends AutoCloseable> it) {
        closeAll(RuntimeException.class, RuntimeException::new, it);
    }
    public static void parallelCloseAll(Collection<? extends AutoCloseable> list) {
        parallelCloseAll(list.iterator());
    }
    public static void parallelCloseAll(Iterator<? extends AutoCloseable> it) {
        parallelCloseAll(RuntimeException.class, RuntimeException::new, it);
    }

    public static <T extends Throwable>
    void closeAll(Class<T> tClass, Function<Throwable, T> factory,
                  Iterator<? extends AutoCloseable> it) throws T {
        T acc = null;
        while (it.hasNext()) {
            var o = it.next();
            try {
                o.close();
            } catch (Throwable t) {
                if (acc == null) //noinspection unchecked
                    acc = tClass.isInstance(t) ? (T)t : factory.apply(t);
                else acc.addSuppressed(t);
            }
        }
        if (acc != null)
            throw acc;
    }

    public static <T extends Throwable>
    void parallelCloseAll(Class<T> tClass, Function<Throwable, T> factory,
                          Iterator<? extends AutoCloseable> it) throws T {
        Throwable[] acc = {null};
        var lock = new ReentrantLock();
        List<Thread> threads = new ArrayList<>();
        while (it.hasNext()) {
            var o = it.next();
            threads.add(Thread.startVirtualThread(() -> {
                try {
                    o.close();
                } catch (Throwable t) {
                    lock.lock();
                    try {
                        if (acc[0] == null) acc[0] = tClass.isInstance(t) ? t : factory.apply(t);
                        else                acc[0].addSuppressed(t);
                    } finally { lock.unlock(); }
                }
            }));
        }
        for (Thread thread : threads)
            Async.uninterruptibleJoin(thread);
        if (acc[0] != null) //noinspection unchecked
            throw (T)acc[0];
    }
}
