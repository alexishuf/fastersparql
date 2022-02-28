package com.github.alexishuf.fastersparql.client.util.reactive;

import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.index.qual.Positive;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
@Accessors(fluent = true)
public class MultiThreadedMergePublisher<T> implements ExecutorBoundPublisher<T> {
    private static final AtomicInteger nextId = new AtomicInteger(1);

    /* --- --- --- Immutable state --- --- --- */
    @Getter private final String name;
    private final CallbackPublisher<T> cbp;
    private final boolean ignoreUpstreamErrors;
    private final BoundedEventLoopPool pool;
    @Getter private final @Positive int maxConcurrency;
    @Getter private final @Positive int tgtConcurrency;

    /* --- --- --- start/termination  state --- --- --- */
    private Throwable terminationCause;
    private boolean completable, terminated, cancelled, subscribed;

    /* --- --- --- request distribution state --- --- --- */
    private boolean redistributing;
    private long undistributed;

    /* --- --- --- Sources  state --- --- --- */
    private int nextSourceNumber = 1, activeSources;
    private final ArrayDeque<Publisher<? extends T>> publishersQueue = new ArrayDeque<>();
    private final IdentityHashMap<Source, Object> sources = new IdentityHashMap<>();

    @Data @Accessors(fluent = true, chain = true)
    public static class Builder {
        private String name;
        private int maxConcurrency = Integer.MAX_VALUE;
        private @Positive int targetConcurrency = 1;
        private boolean ignoreUpstreamErrors;
        private BoundedEventLoopPool pool;

        public Builder concurrency(int targetAndMaxConcurrency) {
            return targetConcurrency(targetConcurrency).maxConcurrency(targetAndMaxConcurrency);
        }

        public <T> MultiThreadedMergePublisher<T> build() {
            return new MultiThreadedMergePublisher<>(name, maxConcurrency, targetConcurrency, ignoreUpstreamErrors, pool);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
    public static Builder eager() {
        return new Builder().maxConcurrency(1).targetConcurrency(1);
    }
    public static Builder concurrent(int targetAndMaxConcurrency) {
        return new Builder().maxConcurrency(targetAndMaxConcurrency)
                            .targetConcurrency(targetAndMaxConcurrency);
    }

    public MultiThreadedMergePublisher(@Nullable String name, int maxConcurrency, int targetConcurrency,
                                       boolean ignoreUpstreamErrors, @Nullable BoundedEventLoopPool pool) {
        this.name = name == null ? "MergePublisher-"+nextId.getAndIncrement() : name;
        this.ignoreUpstreamErrors = ignoreUpstreamErrors;
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency="+maxConcurrency+", expected > 0");
        } if (targetConcurrency < 1) {
            String msg = "targetConcurrency=" + targetConcurrency + ", expected > 0";
            throw new IllegalArgumentException(msg);
        } else if (targetConcurrency == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("targetConcurrency="+targetConcurrency+" too large");
        } else if (targetConcurrency > maxConcurrency) {
            String msg = "expected targetConcurrency <= maxConcurrency: "
                       + targetConcurrency + " > " + maxConcurrency;
            throw new IllegalArgumentException(msg);
        }
        this.maxConcurrency = maxConcurrency;
        this.tgtConcurrency = targetConcurrency;
        this.pool = pool == null ? BoundedEventLoopPool.get() : pool;
        this.cbp = new CallbackPublisher<T>(name, this.pool.chooseExecutor()) {
            @Override protected void onRequest(long n) { redistribute(n, "onRequest"); }
            @Override protected void  onBackpressure() { }
            @Override protected void        onCancel() { tryComplete(null, null, true, 0); }
        };
        // when this terminates with an error, all sources will be cancelled, but until
        // the cancellation takes effect, not failed sources will try to feed() new items, which
        // is not an error.
        this.cbp.silenceFeedAfterCompleteWarnings();
    }

    @Override public void moveTo(Executor executor) {
        cbp.moveTo(executor);
    }

    @Override public Executor executor() {
        return cbp.executor();
    }

    /**
     * Add a upstream {@link Publisher} for consumption.
     *
     * If the number of active upstream {@link Publisher}s exceeds the configured concurrency,
     * the given {@code publisher} will be held in a queue and its
     * {@link Publisher#subscribe(Subscriber)} method will only be called once one of the active
     * upstream publishers terminates.
     *
     * @param publisher the {@link Publisher} to consume.
     */
    public void addPublisher(Publisher<? extends T> publisher) {
        log.trace("{}.addPublisher({})", this, publisher);
        synchronized (this) {
            publishersQueue.add(publisher);
            redistribute(0, "addPublisher()");
        }
    }

    /**
     * Make the publisher completable.
     *
     * When the publisher is completable, as soon as there is no unterminated upstream
     * {@link Publisher} and there is no queued {@link Publisher} ({@link Publisher}s are queued
     * to honor the maximum concurrency set in the constructor), the merge publisher itself will
     * emit {@link Subscriber#onComplete()}.
     *
     * Note that a completion may be emitted from within this method.
     */
    public void markCompletable() {
        boolean changed;
        synchronized (this) {
            changed = !completable;
            if (changed)
                completable = true;
        }
        if (changed && !tryComplete(null, null, false, 0)) {
            log.debug("{}.markCompletable(), activeSources={}, {} queued, undistributed={}",
                      this, activeSources, publishersQueue.size(), undistributed);
        }
    }

    @Override public void subscribe(Subscriber<? super T> s) {
        cbp.subscribe(s);
        synchronized (this) {
            subscribed = true;
            redistribute(0, "subscribe()");
        }
    }

    @Override public String toString() {
        return name;
    }

    /* --- --- --- implementation details --- --- --- */

    private void innerAddPublisher(Publisher<? extends T> publisher) {
        assert activeSources < maxConcurrency;
        Source src = new Source(publisher, nextSourceNumber++);
        ++activeSources;
        sources.put(src, this);
    }

    private boolean tryComplete(@Nullable Throwable cause, @Nullable Source source,
                             boolean cancel, long additionalUndistributed) {
        boolean callComplete, result;
        synchronized (this) {
            // remove the source
            if (!removeSource(source)) return terminated;
            if (!checkUnterminated(cause, source, cancel)) return terminated;

            if (ignoreUpstreamErrors && cause != null)
                log.info("Ignoring upstream error {} from {}", cause, source);
            callComplete = (cause != null && !ignoreUpstreamErrors)
                        || (completable && activeSources == 0 && publishersQueue.isEmpty());
            if (callComplete || cancel) {
                terminationCause = ignoreUpstreamErrors ? null : cause;
                log.debug("{} completing{} with error={} from source={}",
                          this, cancel ? " by cancel()" : "", terminationCause, source);
                terminated = true;
                if (cancel) {
                    cancelled = true;
                    for (Source src : sources.keySet())
                        src.cancel();
                    assert activeSources == sources.size() : "activeSources != #sources";
                    activeSources = 0;
                    sources.clear();
                }
            } else {
                assert !terminated;
                redistribute(additionalUndistributed, "tryComplete");
            }
            result = terminated;
        }
        if (callComplete)
            cbp.complete(terminationCause);
        return result;
    }

    private boolean checkUnterminated(@Nullable Throwable cause, @Nullable Source source,
                                      boolean cancel) {
        if (terminated) {
            String oldCause = Objects.toString(terminationCause);
            if (cancel) {
                if (cancelled)
                    log.debug("Ignoring cancel(): previously cancel()ed");
                else
                    log.debug("Ignoring cancel(): completed with {}", oldCause);
            } else if (cancelled) {
                log.debug("Ignoring {} from {}: previously cancel()ed", cause, source);
            } else {
                log.debug("Ignoring {} from {}: completed with {}", cause, source, oldCause);
            }
            return false;
        }
        return true;
    }

    private boolean removeSource(@Nullable Source source) {
        if (source == null) {
            return true;
        } else if (sources.remove(source) == null) {
            log.error("Completed {} was not an active source, double complete?", source);
            return false;
        } else {
            assert activeSources > 0 : "inconsistent activeSources";
            --activeSources;
        }
        return true;
    }

    /**
     * Add {@code additional} to {@code undistributed} and if the state allows it:
     * <ol>
     *    <li>Create new {@link Source} instances from queued {@link Publisher}s</li>
     *    <li>Distribute work among sources via {@link Source#request(long)}</li>
     * </ol>
     */
    private synchronized void redistribute(long additional, String caller) {
        assert additional >= 0 : "Negative additional";
        undistributed += additional;
        if (redistributeSpecialCases(additional, caller))
            return;
        log.trace("{}.redistribute({}) from {}: updated undistributed={}, activeSources={}",
                  this, additional, caller, undistributed, activeSources);
        redistributing = true;
        long distributed = 0, total = undistributed;
        undistributed = 0;
        try {
            int started = startQueuedPublishers();
            if (started > 0) {
                log.trace("{}.redistribute({}) from {}: started {} sources",
                          this, additional, caller, started);
            }
            distributed = redistributeToSources(total);
        } finally {
            redistributing = false;
            assert distributed <= total;
            if (distributed < total) {
                undistributed += total - distributed;
            }
        }
    }

    /** Distribute {@code total} among {@code request()} calls on {@code sources} */
    private long redistributeToSources(long total) {
        if (total == 0)
            return 0;
        long distributed = 0;
        // If completable, at least one of the following holds:
        //   1. activeSources == tgtConcurrency
        //   2. empty publishersQueue and no future addPublisher() calls
        // Thus, applying using tgtConcurrency > activeSources as div will preserve
        // requests for future publishers that will never come.
        int div = completable ? activeSources : Math.max(tgtConcurrency, activeSources);
        // divide our allowance, total, into chunks and deliver the remainder to the first source
        long chunk = div > total ? 1 : total / div;
        assert chunk > 0;
        long remainder = Math.max(0, total - chunk * div);
        for (Source src : sources.keySet()) {
            long n = Math.min(total-distributed, chunk+remainder);
            if (n <= 0) // stop distributing once distributed >= total
                break;
            if (remainder > 0) // first source consumes the remainder
                remainder = 0;
            distributed += n;
            assert distributed <= total;
            src.request(n);
        }
        assert distributed <= total;
        return distributed;
    }

    /** Handle all cases for {@code redistribute()} that do not call {@code request(n)}. */
    private boolean redistributeSpecialCases(long additional, String caller) {
        if (terminated) {
            log.trace("{}.redistribute({}) from {}: already terminated", this, additional, caller);
        } else if (!subscribed) {
            log.trace("{}.redistribute({}) from {}: not yet subscribe()d",
                      this, additional, caller);
        } else if (redistributing) {
            log.trace("{}.redistribute({}) from {}: reentrant call, adding to undistributed={}",
                      this, additional, caller, undistributed);
            undistributed += additional;
        } else if (undistributed == 0) {
            log.trace("{}.redistribute({}) from {}: nothing to distribute",
                      this, additional, caller);
        } else if (activeSources == 0 && publishersQueue.isEmpty()) {
            log.trace("{}.redistribute({}) from {}: no upstream subscription/publisher",
                      this, additional, caller);
        } else {
            return false; // not a special case
        }
        return true; // handled the special case
    }

    /** Create {@link Source}s from queued {@link Publisher}s as {@code maxConcurrency} allows. */
    private int startQueuedPublishers() {
        int subscribed = 0;
        for (;  activeSources < maxConcurrency && !publishersQueue.isEmpty(); subscribed++)
            innerAddPublisher(publishersQueue.remove());
        if (subscribed > 0)
            log.trace("{}.startQueuedPublishers(): {} new sources", this, subscribed);
        return subscribed;
    }

    private class Source implements Subscriber<T> {
        private final ExecutorBoundPublisher<? extends T> upstreamPublisher;
        private final int number;
        private boolean active = true;
        private long undelivered;
        private @MonotonicNonNull Subscription upstream;

        public Source(Publisher<? extends T> publisher, int number) {
            Executor executor = pool.chooseExecutor();
            this.upstreamPublisher = ExecutorBoundPublisher.bind(publisher, executor);
            this.number = number;
            this.upstreamPublisher.subscribe(this);
        }

        public void request(long n) {
            boolean accept;
            synchronized (this) {
                accept = active;
                if (accept) {
                    if (n > 0) {
                        undelivered += n;
                        upstream.request(n);
                    } else {
                        assert false : "n <= 0";
                    }
                }
            }
            log.trace("{}.request({}): {}", this, n, accept ? "accepted" : "rejected");
            if (!accept)
                redistribute(n, "rejected Source.request");
        }

        public void cancel() {
            boolean accept;
            synchronized (this) {
                accept = active;
                active = false;
            }
            if (accept)
                upstream.cancel();
        }

        private void complete(@Nullable Throwable cause) {
            log.trace("{}.complete({})", this, cause);
            long n;
            synchronized (this) {
                active = false;
                n = undelivered;
                undelivered = 0;
            }
            tryComplete(cause, this, false, n);
        }

        @Override public void onSubscribe(Subscription s)  { upstream = s;         }
        @Override public void      onNext(T item)          { cbp.feed(item);       }
        @Override public void     onError(Throwable cause) { complete(cause);      }
        @Override public void  onComplete()                { complete(null); }

        @Override public String toString() {
            return MultiThreadedMergePublisher.this+"["+number+"="+upstreamPublisher+"]";
        }
    }
}
