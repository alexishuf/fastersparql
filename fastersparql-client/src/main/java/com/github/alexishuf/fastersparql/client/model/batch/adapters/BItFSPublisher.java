package com.github.alexishuf.fastersparql.client.model.batch.adapters;

import com.github.alexishuf.fastersparql.client.model.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.batch.BItClosedException;
import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import com.github.alexishuf.fastersparql.client.util.reactive.BoundedEventLoopPool;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BItFSPublisher<T> implements FSPublisher<T>, Subscription {
    private static final Logger log = LoggerFactory.getLogger(BItFSPublisher.class);

    /* --- --- --- immutable state --- --- --- */
    private final BIt<T> it;
    private final Lock lock = new ReentrantLock(false);

    /* --- --- --- Publisher state (thread safety ensured by API) --- --- --- */
    private @MonotonicNonNull Executor executor;
    private @MonotonicNonNull Subscriber<? super T> subscriber;
    /** Whether cancel() has been called */
    private boolean cancelled;
    /** Whether subscriber's onComplete()/onError() methods have been called */
    private boolean terminated;

    /* --- --- --- shared state (protected by lock) --- --- --- */
    private final Condition hasRequests = lock.newCondition();
    private @NonNegative long requested = 0;
    private @MonotonicNonNull Thread drainer;
    private final ArrayDeque<Batch<T>> batches = new ArrayDeque<>();

    /* --- --- --- feeder state --- --- --- */
    private @NonNegative int firstItem = 0;

    public BItFSPublisher(BIt<T> it) {
        this.it = it;
    }

    @Override public void moveTo(Executor executor) {
        if (executor == null)
            throw new NullPointerException("cannot move to null Executor");
        if (this.executor != null && this.executor.equals(executor))
            return; // no-op
        if (this.subscriber != null)
            throw new IllegalStateException("Cannot moveTo("+executor+") after subscribe()");
        this.executor = executor;
    }

    @Override public Executor executor() {
        return executor;
    }

    @Override public void subscribe(Subscriber<? super T> s) {
        if (subscriber != null) {
            s.onSubscribe(new Subscription() {
                @Override public void request(long n) { }
                @Override public void cancel() { }
            });
            s.onError(new IllegalStateException(this+" already subscribed by "+subscriber));
        } else {
            if (executor == null)
                executor = BoundedEventLoopPool.get().chooseExecutor();
            (subscriber = s).onSubscribe(this);
        }
    }

    @Override public String toString() { return "BItFSPublisher{exec="+executor+", it="+it+"}"; }

    /* --- --- --- Subscription methods --- --- --- */

    @Override public void request(long n) {
        if (n < 0) {
            var ex = new IllegalArgumentException("request(" + n + "): Expected n > 0");
            executor.execute(() -> terminate(ex));
        } else if (n > 0) {
            lock.lock();
            try {
                requested += n;
                if (requested < 0) // handle overflow
                     requested = Long.MAX_VALUE;
                hasRequests.signal();
                if (drainer == null && !cancelled)
                    drainer = Thread.ofVirtual().name("FSPublisherDrainer-"+it).start(this::drain);
            } finally { lock.unlock(); }
        }
    }

    @Override public void cancel() {
        cancelled = true; // suppress future spawning of drainer thread
        it.close();
    }

    /* --- --- --- helper methods --- --- --- */

    private void drain() {
        try (it) {
            for (var b = it.nextBatch(); b.size > 0; b = it.nextBatch()) {
                lock.lock();
                try {
                    while (requested == 0)
                        hasRequests.awaitUninterruptibly();
                    batches.add(b);
                } finally { lock.unlock(); }
                executor.execute(feeder);
            }
            executor.execute(() -> terminate(null));
        } catch (Throwable t) {
            if (cancelled && t instanceof BItClosedException) {
                log.trace("{}: ignoring BItClosedException on drain()", this);
            } else {
                executor.execute(feeder); // deliver any available and requested items
                executor.execute(() -> terminate(t));
            }
        }
    }

    private void terminate(@Nullable Throwable error) {
        if (!terminated) {
            terminated = true;
            if (error == null) subscriber.onComplete();
            else               subscriber.onError(error);
        }
    }

    private final Runnable feeder = () -> {
        if (terminated)
            return;
        while (true) {
            Batch<T> batch;
            int permits;
            lock.lock();
            try {
                if (requested == 0 || batches.isEmpty())
                    return;
                do {
                    batch = batches.poll();
                } while (batch != null && batch.size == 0);
                if (batch == null)
                    return;
                permits = (int)Math.min(requested, batch.size-firstItem);
                requested -= permits;
            } finally { lock.unlock(); }
            T[] a = batch.array;
            int end = firstItem+permits;
            for (int i = firstItem; i < end; i++) {
                try {
                    subscriber.onNext(a[i]);
                } catch (Throwable t) {
                    cancel();
                    terminated = true;
                    firstItem = 0;
                    return;
                }
            }
            if (end == batch.size) {
                firstItem = 0;
            } else {
                firstItem = end;
                lock.lock();
                try { batches.addFirst(batch); } finally { lock.unlock(); }
            }
        }
    };
}
