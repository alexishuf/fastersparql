package com.github.alexishuf.fastersparql.client.util.reactive;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.Executor;

public class MonoPublisher<T> implements FSPublisher<T> {
    private final T value;
    private @MonotonicNonNull Executor executor;

    public MonoPublisher(T value) {
        this.value = value;
    }

    @Override public void moveTo(Executor executor) {
        this.executor = executor;
    }

    @Override public Executor executor() {
        if (executor == null)
            executor = BoundedEventLoopPool.get().chooseExecutor();
        return executor;
    }

    @Override public void subscribe(Subscriber<? super T> s) {
        s.onSubscribe(new Subscription() {
            @Override public void request(long n) { }
            @Override public void cancel() { }
        });
        s.onNext(value);
        s.onComplete();
    }
}
