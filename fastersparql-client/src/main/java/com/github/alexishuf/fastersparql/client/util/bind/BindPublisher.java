package com.github.alexishuf.fastersparql.client.util.bind;

import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.EmptyPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.MergePublisher;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

public class BindPublisher<R> extends MergePublisher<R> {
    private static final Logger log = LoggerFactory.getLogger(BindPublisher.class);

    private final FSPublisher<R> bindingsPublisher;
    private final int bindConcurrency;
    private final Binder<R> binder;

    /* --- --- --- bindings state --- --- --- */
    private @MonotonicNonNull Subscription bindingsSubscription;
    private boolean bindingsActive = false, loggedBindingsNotActive = false;
    private long bindingsRequested = 0;

    /* --- --- --- metrics state --- --- --- */
    protected long start = Long.MAX_VALUE, rows, bindings;
    protected long totalBoundRows, maxBoundRows, unmatchedBinds;

    @Override public void subscribe(Subscriber<? super R> downstream) {
        if (bindingsSubscription == null) {
            start = System.nanoTime();
            bindingsPublisher.moveTo(executor());
            bindingsPublisher.subscribe(bindingsSubscriber);
        }
        super.subscribe(downstream);
    }

    public BindPublisher(FSPublisher<R> bindingsPublisher, int bindConcurrency, Binder<R> binder,
                         @Nullable String name, @Nullable Executor executor) {
        super(name, bindConcurrency, bindConcurrency, false, executor);
        this.bindingsPublisher = bindingsPublisher;
        this.bindConcurrency = bindConcurrency;
        this.binder = binder;
    }

    /* --- --- --- hooks --- --- --- */

    @Override protected void onRequest(long n) {
        super.onRequest(n);
        requestBindings(false);
    }

    @Override protected void feed(R item) {
        ++rows;
        super.feed(item);
    }

    /* --- --- --- bindings subscriber --- --- --- --- */

    /**
     * Issue a {@link Subscription#request(long)} for bindings up to the allowed concurrency.
     *
     * @param completed if true, decrements the count of unfulfilled previous requests
     *                       before computing the new amount to request.
     */
    protected void requestBindings(boolean completed) {
        if (!assertEventThread()) {
            executor().execute(() -> requestBindings(completed));
            return;
        }
        assert bindingsSubscription != null;
        assert bindingsRequested <= bindConcurrency : "bindingsRequested above allowed concurrency";
        if (bindingsActive) {
            if (completed) --bindingsRequested;
            long n = bindConcurrency - bindingsRequested;
            if (n > 0) {
                bindingsRequested += n;
                bindingsSubscription.request(n);
            }
        } else if (!loggedBindingsNotActive) {
            loggedBindingsNotActive = true;
            log.trace("{}.requestBindings({}): bindings subscription not active", this, completed);
        }
    }

    private final Subscriber<R> bindingsSubscriber = new Subscriber<R>() {
        @Override public void onSubscribe(Subscription s) {
            assert bindingsSubscription == null : "already has bindingsSubscription";
            bindingsSubscription = s;
            bindingsActive = true;
        }

        @Override public void onNext(R r) {
            ++bindings;
            FSPublisher<R> publisher = null;
            try {
                publisher = binder.bind(r);
            } catch (Throwable error) {
                addPublisher(new EmptyPublisher<>(error));
            }
            if (publisher != null)
                addPublisher(new BoundProcessor<>(publisher));
        }

        @Override public void onError(Throwable t) {
            log.trace("{}.onError({})", this, t);
            bindingsActive = false;
            addPublisher(new EmptyPublisher<>(t));
        }

        @Override public void onComplete() {
            log.trace("{}.onComplete()", this);
            bindingsActive = false;
            markCompletable();
        }
    };

    /* --- --- --- bound processor --- --- --- --- */

    private final class BoundProcessor<T> extends AbstractProcessor<T, T> {
        private long boundRows;

        public BoundProcessor(FSPublisher<? extends T> source) { super(source); }

        @Override protected void handleOnNext(T row) {
            ++boundRows;
            emit(row);
        }

        @Override protected void onTerminate(@Nullable Throwable error, boolean cancelled) {
            if (boundRows > maxBoundRows)
                maxBoundRows = boundRows;
            totalBoundRows += boundRows;
            if (boundRows == 0)
                ++unmatchedBinds;
            if (!cancelled && error == null)
                requestBindings(true);
        }
    }
}
