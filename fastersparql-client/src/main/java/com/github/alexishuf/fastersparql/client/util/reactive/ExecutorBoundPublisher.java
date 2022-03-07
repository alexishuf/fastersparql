package com.github.alexishuf.fastersparql.client.util.reactive;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.Executor;

public class ExecutorBoundPublisher<T> implements FSPublisher<T> {
    private final Publisher<? extends T> upstreamPub;
    private @MonotonicNonNull Subscription upstream;
    private final CallbackPublisher<T> cbp;
    private final String name;

    public ExecutorBoundPublisher(String name,
                                  Publisher<? extends T> publisher, Executor executor) {
        assert !(publisher instanceof FSPublisher)
                : "Needless wrapping of FastersparqlPublisher";
        this.name = name;
        this.upstreamPub = publisher;
        this.cbp = new CallbackPublisher<T>(name+".cbp", executor) {
            @Override protected void      onRequest(long n) { upstream.request(n); }
            @Override protected void onBackpressure()       { }
            @Override protected void       onCancel()       { upstream.cancel(); }
        };
    }
    public ExecutorBoundPublisher(Publisher<? extends T> upstream, Executor executor) {
        this(upstream.toString(), upstream, executor);
        assert !(upstream instanceof FSPublisher)
                : "Needless wrapping of FastersparqlPublisher";
    }

    @Override public void       moveTo(Executor executor) { cbp.moveTo(executor); }
    @Override public Executor executor()                  { return cbp.executor(); }

    private final Subscriber<T> upstreamSubscriber = new Subscriber<T>() {
        @Override public void onSubscribe(Subscription s)  { upstream = s; }
        @Override public void      onNext(T item)          { cbp.feed(item); }
        @Override public void     onError(Throwable cause) { cbp.complete(cause); }
        @Override public void  onComplete()                { cbp.complete(null); }
        @Override public String toString() {
            return ExecutorBoundPublisher.this+".upstreamSubscriber";
        }
    };

    @Override public void subscribe(Subscriber<? super T> s) {
        if (!cbp.isSubscribed()) {
            upstreamPub.subscribe(upstreamSubscriber);
            assert upstream != null;
        }
        cbp.subscribe(s);
    }

    @Override public String toString() {
        return name;
    }
}
