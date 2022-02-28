package com.github.alexishuf.fastersparql.client.util.reactive;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.Executor;

public class ExecutorBoundPublisherWrapper<T> implements ExecutorBoundPublisher<T> {
    private final Publisher<? extends T> upstream;
    private @MonotonicNonNull Subscription upSubscription;
    private final CallbackPublisher<T> cbp;

    public ExecutorBoundPublisherWrapper(Publisher<? extends T> upstream, Executor executor) {
        this.upstream = upstream;
        cbp = new CallbackPublisher<T>("bound.cbp("+upstream.toString()+")", executor) {
            @Override protected void      onRequest(long n) { upSubscription.request(n); }
            @Override protected void onBackpressure()       { }
            @Override protected void       onCancel()       { upSubscription.cancel(); }
        };
    }

    @Override public void moveTo(Executor executor) {
        if (upstream instanceof ExecutorBoundPublisher)
            ((ExecutorBoundPublisher<?>) upstream).moveTo(executor);
        cbp.moveTo(executor);
    }

    @Override public Executor executor() {
        return cbp.executor();
    }

    @Override public void subscribe(Subscriber<? super T> downstream) {
        upstream.subscribe(new Subscriber<T>() {
            @Override public void onSubscribe(Subscription s) { upSubscription = s;  }
            @Override public void     onNext(T item)          { cbp.feed(item);      }
            @Override public void    onError(Throwable cause) { cbp.complete(cause); }
            @Override public void onComplete()                { cbp.complete(null);  }
        });
        cbp.subscribe(downstream);
    }

    @Override public String toString() {
        return "bound("+ upstream.toString()+")";
    }
}
