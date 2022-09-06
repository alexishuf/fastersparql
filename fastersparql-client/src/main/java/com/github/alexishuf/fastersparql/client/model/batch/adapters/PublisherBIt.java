package com.github.alexishuf.fastersparql.client.model.batch.adapters;

import com.github.alexishuf.fastersparql.client.model.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.batch.base.BufferedBIt;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.locks.Condition;

/** Wrap a reactivestreams {@link Publisher} as a {@link BIt} */
public class PublisherBIt<T> extends BufferedBIt<T> {
    private final Publisher<T> publisher;
    private final Condition hasRequest = lock.newCondition();
    private @MonotonicNonNull Subscription subscription;
    private boolean started;
    private long pendingRequest;

    public PublisherBIt(Publisher<T> publisher, Class<T> elementClass) {
        this(publisher, elementClass, publisher.toString());
    }

    public PublisherBIt(Publisher<T> publisher, Class<T> elementClass,
                        @Nullable String name) {
        super(elementClass, name == null ? "BatchIt("+publisher.toString()+")" : name);
        this.publisher = publisher;
    }

    public static final class CancelledException extends RuntimeException {
        public CancelledException(String message) { super(message); }
    }

    /* --- --- --- helper methods --- --- --- */

    private final class BatchingSubscriber implements Subscriber<T> {
        @Override public void onSubscribe(Subscription s) {
            lock.lock();
            try {
                PublisherBIt.this.subscription = s;
                hasReady.signalAll();
            } finally { lock.unlock(); }
        }

        @Override public void onNext(T t)          { feed(t); }
        @Override public void onError(Throwable t) { complete(t); }
        @Override public void onComplete()         { complete(null); }
    }

    /**
     * Block until {@link BatchingSubscriber#onSubscribe(Subscription)} is called.
     *
     * @return the {@link Subscription} or {@code null} if already {@code complete()}d. */
    private @Nullable Subscription awaitSubscription() {
        lock.lock();
        try {
            while (subscription == null && !ended)
                hasReady.awaitUninterruptibly();
            return ended ? null : subscription;
        } finally { lock.unlock(); }

    }

    /** Worker thread entry point: issues {@code request(n)} calls until {@code complete()}d */
    private void work() {
        publisher.subscribe(new BatchingSubscriber());
        Subscription subscription = awaitSubscription();
        if (subscription == null)
            return; // complete() already called

        // issue request() calls
        long n;
        while (true) {
            lock.lock();
            try {
                while (!ended && pendingRequest == 0)
                    hasRequest.awaitUninterruptibly();
                if (ended)
                    return; // complete() called by upstream or close()/cleanup()
                n = pendingRequest;
                pendingRequest = 0;
            } finally { lock.unlock(); }
            // request() may block/compute, thus this thread must release the lock
            subscription.request(n);
        }
    }

    @Override protected void waitReady() {
        if (!started) {
            started = true;
            Thread.ofVirtual().name(publisher+"-worker").start(this::work);
        }
        if (ready.isEmpty()) {
            pendingRequest += maxBatch;
            hasRequest.signalAll();
        }
        super.waitReady();
    }

    /* --- --- --- implementations --- --- --- */

    @Override protected void cleanup() {
        Subscription mustCancel = null;
        lock.lock();
        try {
            if (started && !ended) {
                complete(new CancelledException("Publisher cancelled due to "+this+".cancel()"));
                mustCancel = awaitSubscription();
            }
        } finally { lock.unlock(); }
        if (mustCancel != null)
            mustCancel.cancel();
    }
}
