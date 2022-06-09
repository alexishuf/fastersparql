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

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.currentThread;


@Slf4j
@Accessors(fluent = true)
public class MergePublisher<T> implements FSPublisher<T> {
    private static final AtomicInteger nextId = new AtomicInteger(1);

    /* --- --- --- Immutable state --- --- --- */
    @Getter private final String name;
    private final CallbackPublisher<T> cbp;
    private final boolean ignoreUpstreamErrors;
    @Getter private final @Positive int maxConcurrency;
    @Getter private final @Positive int tgtConcurrency;

    /* --- --- --- thread-safety/reentrancy assertions --- --- --- */
    private final AtomicBoolean completing     = new AtomicBoolean();
    private final AtomicBoolean redistributing = new AtomicBoolean();
    private final AtomicReference<Thread> evThread = new AtomicReference<>();

    /* --- --- --- start/termination  state --- --- --- */
    private final Lock subscribeLock = new ReentrantLock(); // synchronizes all public methods
    private Throwable terminationCause;
    private boolean terminated, cancelled, completable, subscribed;

    /* --- --- --- request distribution state --- --- --- */
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
        private Executor executor;

        public Builder concurrency(int targetAndMaxConcurrency) {
            return targetConcurrency(targetConcurrency).maxConcurrency(targetAndMaxConcurrency);
        }

        public <T> MergePublisher<T> build() {
            return new MergePublisher<>(name, maxConcurrency, targetConcurrency,
                                         ignoreUpstreamErrors, executor);
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

    public MergePublisher(@Nullable String name, int maxConcurrency, int targetConcurrency,
                          boolean ignoreUpstreamErrors, @Nullable Executor executor) {
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
        executor = executor == null ? BoundedEventLoopPool.get().chooseExecutor() : executor;

        this.cbp = new CallbackPublisher<T>(name, executor) {
            @Override protected void onRequest(long n) { MergePublisher.this.onRequest(n); }
            @Override protected void  onBackpressure() { }
            @Override protected void        onCancel() { MergePublisher.this.onCancel(); }
        };
        // when this terminates with an error, all sources will be cancelled, but until
        // the cancellation takes effect, not failed sources will try to feed() new items, which
        // is not an error.
        this.cbp.silenceFeedAfterCompleteWarnings();
    }

    @Override public void moveTo(Executor executor) {
        subscribeLock.lock();
        try {
            if (subscribed)
                throw new IllegalStateException("cannot move executor after subscribed");
            cbp.moveTo(executor);
        } finally {
            subscribeLock.unlock();
        }
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
        subscribeLock.lock();
        try {
            if (subscribed)
                executor().execute(() -> addPublisherTask(publisher));
            else
                publishersQueue.add(publisher);
        } finally {
            subscribeLock.unlock();
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
        subscribeLock.lock();
        try {
            if (subscribed)
                executor().execute(markCompletableTask);
            else
                completable = true;
        } finally {
            subscribeLock.unlock();
        }
    }

    @Override public void subscribe(Subscriber<? super T> s) {
        subscribeLock.lock();
        try {
            subscribed = true;
            cbp.subscribe(s);                   // will notify s if already subscribed
            executor().execute(subscribedTask); // no bad effect if already subscribed
        } finally {
            subscribeLock.unlock();
        }
    }

    @Override public String toString() {
        return name;
    }


    /* --- --- --- protected methods overridable by subclasses --- --- --- */

    protected void onRequest(long n) {
        redistribute(n, "onRequest");
    }
    protected   void onCancel() {
        tryComplete(null, null, true, 0);
    }
    protected void onComplete(Throwable cause, boolean cancelled) { }
    protected void       feed(T item)                             { cbp.feed(item); }

    /* --- --- --- in-executor tasks for public interface methods --- --- --- */

    private void addPublisherTask(Publisher<? extends T> publisher) {
        assertEventThread();
        publishersQueue.add(publisher);
        redistribute(0, "addPublisher()");
    }

    private final Runnable markCompletableTask = () -> {
        assertEventThread();
        if (!completable) {
            completable = true;
            if (!tryComplete(null, null, false, 0)) {
                log.trace("{}.markCompletable(), activeSources={}, {} queued, undistributed={}",
                          this, activeSources, publishersQueue.size(), undistributed);
            }
        }
    };

    private final Runnable subscribedTask = () -> {
        assertEventThread();
        if (completable)
            tryComplete(null, null, false, 0);
        redistribute(0, "subscribe()");
    };


    /* --- --- --- implementation details --- --- --- */

    /**
     * Assert (not simply boolean test) that this call is being made from the event thread.
     *
     * Calling this from a thread which is not the event thread will log an error
     * (and intentionally raise an {@link AssertionError} if asserts are enabled).
     *
     * @return True if called from the event thread, false if not (and asserts are disabled).
     * @throws AssertionError if not in the event thread and asserts are enabled.
     */
    protected boolean assertEventThread() {
        Thread me = currentThread();
        if (!evThread.compareAndSet(null, me) && !me.equals(evThread.get())) {
            log.error("Bad thread in {} critical section, expected {}", this, evThread.get());
            assert false : "Concurrent access to critical section";
            return false;
        }
        return true;
    }

    private boolean isConcurrentOrReentrant(AtomicBoolean flag) {
        return !assertEventThread() || !flag.compareAndSet(false, true);
    }

    /**
     * If terminated or cancelled, is a no-op. Else terminates or cancels if rules allow. If
     * rules do not allow termination, calls {@code redistribute(additionalUndistributed)}.
     */
    private boolean tryComplete(@Nullable Throwable cause, @Nullable Source source,
                                boolean cancel, long additionalUndistributed) {
        if (isConcurrentOrReentrant(completing)) {
            executor().execute(() -> tryComplete(cause, source, cancel, additionalUndistributed));
        } else {
            try {
                // trivial cases (but lengthy log logic)
                if (!removeSource(source)) return terminated;
                if (!checkUnterminated(cause, source, cancel)) return terminated;

                if (ignoreUpstreamErrors && cause != null)
                    log.info("Ignoring upstream error {} from {}", cause, source);

                boolean complete = (cause != null && !ignoreUpstreamErrors)
                        || (completable && activeSources == 0 && publishersQueue.isEmpty());
                if (complete || cancel) {
                    terminationCause = ignoreUpstreamErrors ? null : cause;
                    log.trace("{} completing{} with error={} from source={}",
                            this, cancel ? " by cancel()" : "", terminationCause, source);
                    terminated = true;
                    if (cancel) {
                        cancelled = true;
                        for (Source src : sources.keySet())
                            src.cancel();
                        assert activeSources == sources.size() : "activeSources != #sources";
                        activeSources = 0;
                        sources.clear();
                        publishersQueue.clear();
                    }
                    cbp.complete(terminationCause);
                    onComplete(terminationCause, cancel);
                } else {
                    assert !terminated;
                    redistribute(additionalUndistributed, "tryComplete");
                }
            } finally {
                completing.set(false);
            }
        }
        return terminated;
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
     * Add {@code additional} to {@code undistributed} and if the state allows:
     * <ol>
     *    <li>Create new {@link Source} instances from queued {@link Publisher}s</li>
     *    <li>Distribute work among sources via {@link Source#request(long)}</li>
     * </ol>
     */
    private void redistribute(long additional, String caller) {
        assert additional >= 0 : "Negative additional";
        if (isConcurrentOrReentrant(redistributing)) {
            executor().execute(() -> redistribute(additional, caller));
        } else {
            try {
                undistributed += additional;
                if (redistributeSpecialCases(additional, caller))
                    return;
                log.trace("{}.redistribute({}) from {}: updated undistributed={}, activeSources={}",
                        this, additional, caller, undistributed, activeSources);
                long distributed = 0, total = undistributed;
                undistributed = 0;
                try {
                    startQueuedPublishers();
                    distributed = redistributeToSources(total, caller);
                } finally {
                    assert distributed <= total;
                    if (distributed < total)
                        undistributed += total - distributed;
                }
            } finally {
                redistributing.set(false);
            }
        }
    }

    /** Distribute {@code total} among {@code request()} calls on {@code sources} */
    private long redistributeToSources(long total, String caller) {
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
        boolean rejected = false;
        for (Source src : sources.keySet()) {
            long n = Math.min(total-distributed, chunk+remainder);
            if (n <= 0) // stop distributing once distributed >= total
                break;
            if (remainder > 0) // first source consumes the remainder
                remainder = 0;
            if (src.request(n)) {
                distributed += n;
                assert distributed <= total;
            } else {
                rejected = true;
            }
        }
        assert distributed <= total;
        if (rejected)
            executor().execute(() -> redistribute(0, caller));
        return distributed;
    }

    /** Handle all cases for {@code redistribute()} that do not call {@code request(n)}. */
    private boolean redistributeSpecialCases(long additional, String caller) {
        if (terminated) {
            log.trace("{}.redistribute({}) from {}: already terminated", this, additional, caller);
        } else if (!subscribed) {
            log.trace("{}.redistribute({}) from {}: not yet subscribe()d",
                      this, additional, caller);
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
    private void startQueuedPublishers() {
        if (activeSources >= maxConcurrency || publishersQueue.isEmpty())
            return; // no work to do
        List<Source> added = new ArrayList<>(maxConcurrency-activeSources);
        while (activeSources < maxConcurrency && !publishersQueue.isEmpty()) {
            Source src = new Source(publishersQueue.remove(), nextSourceNumber++);
            ++activeSources;
            sources.put(src, this);
            added.add(src);
        }
        for (Source src : added)
            src.subscribe();
        log.trace("{}.startQueuedPublishers(): {} new sources", this, added.size());
    }

    private class Source implements Subscriber<T> {
        private final FSPublisher<? extends T> upstreamPublisher;
        private final int number;
        private boolean active = false, cancelled;
        private long undelivered;
        private @MonotonicNonNull Subscription upstream;
        private @MonotonicNonNull Thread subscriberThread;
        private @MonotonicNonNull Thread onSubscribeThread;

        public Source(Publisher<? extends T> publisher, int number) {
            this.upstreamPublisher = FSPublisher.bind(publisher, executor());
            this.number = number;
        }

        public void subscribe() {
            if (active)
                throw new IllegalStateException("Already subscribed!");
            subscriberThread = currentThread();
            active = true;
            this.upstreamPublisher.subscribe(this);
        }

        public boolean request(long n) {
            assert currentThread() == subscriberThread;
            assert currentThread() == onSubscribeThread;
            if (!active) {
                return false;
            } else if (n <= 0) {
                log.error("{}.request({}): n <= 0", this, n);
                return false;
            } else {
                log.trace("{}.request({})", this, n);
                undelivered += n;
                upstream.request(n);
            }
            return true;
        }

        public void cancel() {
            if (active) {
                log.trace("{}.cancel()", this);
                active = false;
                cancelled = true;
                upstream.cancel();
            }
        }

        private void complete(@Nullable Throwable cause) {
            log.trace("{}.complete({})", this, cause);
            assert cancelled || active : "!cancelled && !active: double completion?";
            if (active) {
                active = false;
                long n = undelivered;
                undelivered = 0;
                tryComplete(cause, this, false, n);
            }
        }

        @Override public void onSubscribe(Subscription s)  {
            onSubscribeThread = currentThread();
            upstream = s;
        }
        @Override public void onNext(T item)          {
            if (undelivered == 0)
                log.warn("{}.onNext({}) beyond requested.", this, item);
            else
                --undelivered;
            feed(item);
        }
        @Override public void     onError(Throwable cause) { complete(cause);      }
        @Override public void  onComplete()                { complete(null); }

        @Override public String toString() {
            return MergePublisher.this+"["+number+"="+upstreamPublisher+"]";
        }
    }
}
