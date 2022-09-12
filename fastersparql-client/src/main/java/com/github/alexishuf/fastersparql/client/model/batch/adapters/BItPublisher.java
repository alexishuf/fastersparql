package com.github.alexishuf.fastersparql.client.model.batch.adapters;

import com.github.alexishuf.fastersparql.client.model.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class BItPublisher<T> implements Publisher<T>, Subscription {
    private final BIt<T> it;
    private @MonotonicNonNull Subscriber<? super T> subscriber;
    private @Nullable Batch<T> residual;
    private int residualStart;
    private long requested;
    private boolean terminated;
    private @Nullable Thread requestThread;

    public BItPublisher(BIt<T> it) {
        this.it = it;
    }

    @Override public void subscribe(Subscriber<? super T> s) {
        if (subscriber == null) {
            (subscriber = s).onSubscribe(this);
        } else {
            s.onSubscribe(new Subscription() {
                @Override public void request(long n) { }
                @Override public void cancel() { }
            });
            s.onError(new IllegalStateException("Already subscribed by "+subscriber));
        }
    }

    @Override public String toString() { return "Publisher("+it+")"; }

    @Override public void request(long n) {
        if (terminated) {
            return;
        } else if (n < 0) {
            subscriber.onError(new IllegalArgumentException("request("+n+"): expected n > 0"));
            terminated = true;
            return;
        }
        requested += n;
        Thread caller = Thread.currentThread();
        if (requestThread == null) {
            requestThread = caller;
            try {
                runRequestLoop();
            } catch (Throwable t) {
                subscriber.onError(t);
                terminated = true;
            } finally {
                requestThread = null;
            }
        } else if (requestThread != caller) {
            throw new IllegalStateException("Concurrent request()");
        }
    }

    private void runRequestLoop() {
        int start = residual == null ? 0 : residualStart;
        var b = residual == null ? it.nextBatch() : residual;
        while (b.size > 0 && requested > 0) {
            int end = (int)Math.min(start+requested, b.size);
            var array = b.array;
            for (int i = start; i < end; i++) {
                try {
                    --requested;
                    subscriber.onNext(array[i]); // may update requested
                } catch (Throwable t) {
                    cancel();
                    return;
                }
            }
            if (end == b.size) {
                b = it.nextBatch(b);
                start = 0;
            } else if (requested > 0) {
                start = end;
            } else {
                residual = b;
                residualStart = end;
            }
        }
        if (b.size == 0) {
            subscriber.onComplete();
            terminated = true;
        }
    }

    @Override public void cancel() { it.close(); }
}
