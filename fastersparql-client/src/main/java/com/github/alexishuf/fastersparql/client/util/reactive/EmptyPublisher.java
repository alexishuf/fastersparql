package com.github.alexishuf.fastersparql.client.util.reactive;

import lombok.ToString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A {@link Publisher} that immediately calls {@link Subscriber#onError(Throwable)} or
 * {@link Subscriber#onComplete()} after {@link Subscriber#onSubscribe(Subscription)} returns
 *
 * @param <T> the declared type of elements produced, even tough no element will ever be delivered.
 */
@ToString
public class EmptyPublisher<T> implements Publisher<T> {
    private final @Nullable Throwable cause;

    /**
     * Creates an {@link Publisher} that calls {@link Subscriber#onComplete()} after
     * {@link Subscriber#onSubscribe(Subscription)}
     */
    public EmptyPublisher() { this(null); }

    /**
     * Creates a {@link Publisher} that ends after {@link Subscriber#onSubscribe(Subscription)}.
     *
     * @param cause if non-null, will call {@link Subscriber#onError(Throwable)} with {@code cause},
     *              else will call {@link Subscriber#onComplete()}.
     */
    public EmptyPublisher(@Nullable Throwable cause) { this.cause = cause; }

    @Override public void subscribe(Subscriber<? super T> s) {
        s.onSubscribe(new Subscription() {
            @Override public void request(long n) {}
            @Override public void cancel() { }
        });
        if (cause != null) s.onError(cause);
        else               s.onComplete();
    }
}
