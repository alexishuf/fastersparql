package com.github.alexishuf.fastersparql.client.util.reactive;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractProcessor<U, D> implements Processor<U, D> {
    private static final Logger log = LoggerFactory.getLogger(AbstractProcessor.class);

    private final Publisher<? extends U> source;
    protected AtomicBoolean terminated = new AtomicBoolean(false);
    protected Subscription upstream;
    protected Subscriber<? super D> downstream;

    public AbstractProcessor(Publisher<? extends U> source) {
        this.source = source;
    }

    /* --- --- --- Behavior-changing methods --- --- --- */

    /**
     * Creates a {@link Subscription} for the downstream {@link Subscriber}.
     *
     * The default implementation creates a {@link Subscription} that forward calls to
     * {@link AbstractProcessor#upstream} if this Processor is not
     * {@link AbstractProcessor#terminated}.
     *
     * @return a non-null {@link Subscription}.
     */
    protected Subscription createDownstreamSubscription() {
        return new Subscription() {
            @Override public void request(long n) {
                if (!terminated.get())
                    upstream.request(n);
            }
            @Override public void cancel() {
                if (terminated.compareAndSet(false, true))
                    cancelUpstream();
            }
        };
    }

    /**
     * Throwing body for {@link Subscriber#onNext(Object)}. Implementations should call
     * {@link AbstractProcessor#emit(Object)} to send elements downstream
     *
     * If anything is throw, the processor will terminate:
     * <ul>
     *     <li>The upstream subscription will be cancelled</li>
     *     <li>New items received from upstream will be silently dropped</li>
     *     <li>The downstream will receive the Throwable via {@link Subscriber#onError(Throwable)}</li>
     * </ul>
     *
     * @param item the item to process.
     */
    protected abstract void handleOnNext(U item) throws Exception;

    /* --- --- --- Helper methods called by implementations --- --- --- */

    /**
     * Call {@code downstream.onNext(item)} and handle eventual {@link Throwable}.
     *
     * @param item the item to publish
     */
    protected void emit(D item) {
        try {
            if (terminated.get()) {
                log.debug("Discarding emit({}) after terminated.", item);
            } else {
                log.trace("{}.emit({})", this, item);
                downstream.onNext(item);
            }
        } catch (Throwable t) {
            assert false : "Unexpected Throwable";
            log.error("downstream={} threw {} on onNext({}). Treating as a Subscription.cancel().",
                      downstream, t.getClass(), item, t);
            cancelUpstream();
        }
    }

    /**
     * Call {@code upstream.cancel()} and handle any {@link Throwable}
     */
    protected void cancelUpstream() {
        try {
            upstream.cancel();
        } catch (Throwable t) {
            assert false : "Unexpected Throwable";
            log.error("Ignoring {} thrown by cancel() of upstream={}", t.getClass(), upstream, t);
        }
    }

    /**
     * Update {@code terminated} and call {@code downstream.cancel()} handling any {@link Throwable}
     *
     * This method is idempotent: If {@code terminated == true} for whatever reason, nothing
     * will happen.
     */
    protected void completeDownstream(@Nullable Throwable cause) {
        log.trace("{}.completeDownstream({})", this, cause);
        try {
            if (terminated.compareAndSet(false, true)) {
                if (cause != null)
                    downstream.onError(cause);
                else
                    downstream.onComplete();
            }
        } catch (Throwable t) {
            assert false : "Unexpected Throwable";
            log.error("Ignoring {} thrown by onComplete() of downstream={}",
                      t.getClass(), downstream, t);
        }
    }

    /* --- --- --- Publisher methods --- --- --- */

    @Override public void subscribe(Subscriber<? super D> s) {
        if (upstream != null) {
            s.onSubscribe(new Subscription() {
                @Override public void request(long n) { }
                @Override public void cancel() { }
            });
            s.onError(new IllegalStateException("Multiple subscribers are not allowed for "+this));
        } else {
            downstream = s;
            source.subscribe(this);
        }
    }

    /* --- --- --- Subscriber methods --- --- --- */

    @Override public void onSubscribe(Subscription s) {
        upstream = s;
        downstream.onSubscribe(createDownstreamSubscription());
    }

    @Override public void onNext(U item) {
        try {
            if (!terminated.get())
                handleOnNext(item);
        } catch (Throwable t) {
            cancelUpstream();
            completeDownstream(t);
        }
    }

    @Override public void onError(Throwable t) {
        completeDownstream(t != null ? t : new Exception("Unknown cause (onError(null) call)"));
    }

    @Override public void onComplete() {
        completeDownstream(null);
    }
}
