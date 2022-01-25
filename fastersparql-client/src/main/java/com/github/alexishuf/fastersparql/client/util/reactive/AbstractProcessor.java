package com.github.alexishuf.fastersparql.client.util.reactive;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractProcessor<T> implements Processor<T, T> {
    private static final Logger log = LoggerFactory.getLogger(AbstractProcessor.class);

    private final Publisher<? extends T> source;
    protected boolean terminated;
    protected Subscription upstream;
    protected Subscriber<? super T> downstream;

    public AbstractProcessor(Publisher<? extends T> source) {
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
                if (!terminated)
                    upstream.request(n);
            }
            @Override public void cancel() {
                if (!terminated)  {
                    terminated = true;
                    upstream.cancel();
                }
            }
        };
    }

    /* --- --- --- Helper methods called by implementations --- --- --- */

    /**
     * Call {@code downstream.onNext(item)} and handle eventual {@link Throwable}.
     *
     * @param item the item to publish
     */
    protected void emit(T item) {
        try {
            downstream.onNext(item);
        } catch (Throwable t) {
            assert false : "Unexpected Throwable";
            log.error("downstream={} threw {} on onNext({}). Treating as a Subscription.cancel().",
                      downstream, t.getClass(), item, t);
            terminated = true;
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
        try {
            if (!terminated) {
                terminated = true;
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

    @Override public void subscribe(Subscriber<? super T> s) {
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

    @Override public void onError(Throwable t) {
        completeDownstream(t != null ? t : new Exception("Unknown cause (onError(null) call)"));
    }

    @Override public void onComplete() {
        completeDownstream(null);
    }
}
