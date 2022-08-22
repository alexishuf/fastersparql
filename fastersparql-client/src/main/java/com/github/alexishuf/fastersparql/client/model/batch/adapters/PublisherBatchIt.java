package com.github.alexishuf.fastersparql.client.model.batch.adapters;

import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import com.github.alexishuf.fastersparql.client.model.batch.BatchIt;
import com.github.alexishuf.fastersparql.client.model.batch.base.BufferedBatchIt;
import com.github.alexishuf.fastersparql.client.util.async.RuntimeExecutionException;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.locks.Condition;

/** Wrap a reactivestreams {@link Publisher} as a {@link BatchIt} */
public class PublisherBatchIt<T> extends BufferedBatchIt<T> {
    private final Publisher<T> publisher;
    private final Condition hasRequest = lock.newCondition();
    private @MonotonicNonNull Subscription subscription;
    private boolean started, cancelled;
    private long pendingRequest;

    public PublisherBatchIt(Publisher<T> publisher, Class<T> elementClass) {
        this(publisher, elementClass, publisher.toString());
    }

    public PublisherBatchIt(Publisher<T> publisher, Class<T> elementClass,
                            @Nullable String name) {
        super(elementClass, name == null ? "BatchIt("+publisher.toString()+")" : name);
        this.publisher = publisher;
    }

    /* --- --- --- helper methods --- --- --- */

    private Subscription awaitSubscription() {
        lock.lock();
        try {
            while (subscription == null && !ended && error == null)
                full.awaitUninterruptibly();
            if (error != null)
                throw new RuntimeExecutionException(error);
            else if (ended)
                throw new IllegalStateException(publisher+" ended without calling onSubscribe");
            return subscription;
        } finally { lock.unlock(); }
    }

    private void start() {
        lock.lock();
        try {
            if (started)
                return; // already started
            started = true;
        } finally { lock.unlock(); }
        Thread.ofVirtual().name(publisher+"-worker").start(this::work);
    }

    private final class BatchingSubscriber implements Subscriber<T> {
        @Override public void onSubscribe(Subscription s) {
            lock.lock();
            try {
                PublisherBatchIt.this.subscription = s;
                full.signalAll();
            } finally { lock.unlock(); }
        }

        @Override public void onNext(T t)          { feed(t); }
        @Override public void onError(Throwable t) { complete(t); }
        @Override public void onComplete()         { complete(null); }
    }

    private void work() {
        publisher.subscribe(new BatchingSubscriber());
        Subscription subscription = awaitSubscription();
        long n;
        lock.lock();
        while (!ended && !cancelled) {
            try {
                while (pendingRequest == 0)
                    hasRequest.awaitUninterruptibly();
                n = pendingRequest;
                pendingRequest = 0;
            } finally { lock.unlock(); }
            subscription.request(n);
            lock.lock();
        }
    }


    @Override protected Batch<T> fetch() {
        start();
        lock.lock();
        try {
            pendingRequest += maxBatch;
            hasRequest.signalAll();
            return super.fetch();
        } finally {
            lock.unlock();
        }

    }

    /* --- --- --- implementations --- --- --- */

    @Override protected void cleanup() {
        boolean active;
        lock.lock();
        try {
            active = subscription != null && !ended && !cancelled;
            if (active)
                cancelled = true;
        } finally { lock.unlock(); }
        if (active)
            subscription.cancel();
    }
}
