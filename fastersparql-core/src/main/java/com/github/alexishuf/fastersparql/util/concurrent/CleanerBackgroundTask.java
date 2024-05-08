package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;

import java.util.Objects;
import java.util.function.Supplier;

public abstract class CleanerBackgroundTask<T>
        extends SingleThreadBackgroundTask<T, MpmcUnboundedXaddArrayQueue<T>> {
    public @MonotonicNonNull Alloc<T> pool;
    public Supplier<T> newInstance;
    public ClearElseMake clearElseMake;

    public CleanerBackgroundTask(String name, Supplier<T> maker) {
        this(name, maker, NORM_PRIORITY);
    }

    public CleanerBackgroundTask(String name, Supplier<T> maker, int priority) {
        super(name, new MpmcUnboundedXaddArrayQueue<>(PREFERED_QUEUE_CHUNK), priority);
        this.newInstance = maker;
        this.clearElseMake = new ClearElseMake(maker);
    }

    public final class ClearElseMake implements Supplier<T> {
        private final Supplier<T> maker;

        public ClearElseMake(Supplier<T> maker) {this.maker = maker;}

        @Override public T get() {
            T o = work.relaxedPoll();
            if (o == null) {
                onSpinWait();
                o = work.relaxedPoll();
                if (o == null)
                    return Objects.requireNonNullElseGet(pool.poll(), maker);
            }
            clear(o);
            return o;
        }
    }

    protected abstract void clear(T obj);

    @Override protected void handle(T o) {
        clear(o);
        pool.offerToShared(o);
    }
}
