package com.github.alexishuf.fastersparql.util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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

    public <V> boolean complete(CompletableFuture<V> future, @Nullable V value) {
        return acc == null ? future.complete(value) : future.completeExceptionally(acc);
    }

    public static void closeAll(Collection<? extends AutoCloseable> list) {
        closeAll(RuntimeException.class, RuntimeException::new, list);
    }

    public static <T extends Throwable>
    void closeAll(Class<T> tClass, Function<Throwable, T> factory,
                  Collection<? extends AutoCloseable> list) throws T {
        T acc = null;
        for (var o : list) {
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
}
