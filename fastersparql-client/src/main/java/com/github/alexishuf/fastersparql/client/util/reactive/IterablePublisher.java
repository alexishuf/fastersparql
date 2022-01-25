package com.github.alexishuf.fastersparql.client.util.reactive;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Iterator;

@Value
public class IterablePublisher<T> implements Publisher<T> {
    Iterable<T> iterable;

    @Override public void subscribe(Subscriber<? super T> s) {
        IteratorSubscription<T> sub = new IteratorSubscription<>(iterable.iterator(), s);
        s.onSubscribe(sub);
    }

    @RequiredArgsConstructor
    private static final class IteratorSubscription<U> implements Subscription {
        private final Iterator<U> iterator;
        private final Subscriber<? super U> downstream;
        private boolean terminated;

        @Override public void request(long n) {
            for (int i = 0; iterator.hasNext() && i < n; i++) {
                U item;
                try {
                    item = iterator.next();
                } catch (Throwable t) {
                    terminated = true;
                    downstream.onError(t);
                    return;
                }
                try {
                    downstream.onNext(item);
                } catch (Throwable t) {
                    terminated = true; // treat as cancel()ed
                    throw t;
                }
            }
            if (!iterator.hasNext() && !terminated) {
                terminated = true;
                downstream.onComplete();
            }
        }

        @Override public void cancel() {
            terminated = true;
        }
    }
}
