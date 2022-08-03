package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.util.CircularBuffer;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

public class TeeProcessor<T> implements Processor<T, T>, FSPublisher<T> {
    private static final Logger log = LoggerFactory.getLogger(TeeProcessor.class);

    private final FSPublisher<? extends T> source;
    private Subscription upstream;
    private final List<DownstreamState> downstream = new ArrayList<>();
    private final CircularBuffer<T> history = new CircularBuffer<>(1<<16);
    private int startAfterSubscribers = Integer.MAX_VALUE;
    private @MonotonicNonNull Throwable error;
    private boolean complete, suspended, suspendedSubscribe;
    private long minConsumed = 0, upstreamRequested, suspendedUpstreamRequests;
    private @Nullable Level lostItemLogLevel = Level.ERROR;

    public static final class MissingHistoryException extends IllegalStateException {
        public MissingHistoryException(TeeProcessor<?> tee, Subscriber<?> subscriber, long lost) {
            super(String.format("%d items are lost from the TeeProcessor buffer since the " +
                                "subscriber subscribed after start(). subscriber=%s, tee=%s",
                                lost, subscriber, tee));
        }
    }

    /* --- --- --- construction --- --- --- */

    public TeeProcessor(FSPublisher<? extends T> source) {
        this.source = source;
        this.suspended = true;
    }

    /* --- --- --- FSPublisher --- --- --- */

    @Override public void moveTo(Executor executor) { source.moveTo(executor); }
    @Override public Executor executor()            { return source.executor();}

    /* --- --- --- TeeProcessor methods --- --- --- */

    /**
     * Enable subscription and requests to the upstream and deliveries to downstream subscribers.
     *
     * <p> For safety reasons, {@link TeeProcessor}s are created in a <strong>suspended</strong> state,
     * which allows downstream subscriptions and silently accepts requests but does not subscribe
     * to the upstream, nor send requests. This suspended state allow all downstream
     * {@link Subscriber}s to subscribe before streaming begins. </p>
     *
     * <p> Calling {@link TeeProcessor#subscribe(Subscriber)} after {@code start()} incurs the risk
     * that the subscription occurs after {@code n} elements have been delivered the previous
     * subscribers and not present anymore on the {@link TeeProcessor} history buffer. This
     * situation will cause an error message to be logged by default. </p>
     *
     * <p>To change the behavior when items are lost in the situation described above, see
     * {@link TeeProcessor#logOnLostItems(Level)} and {@link TeeProcessor#errorOnLostItems}.</p>
     *
     * @return {@code this} {@link TeeProcessor}.
     */
    @SuppressWarnings("UnusedReturnValue")
    public TeeProcessor<T> start() {
        executor().execute(this::doStart);
        return this;
    }

    /**
     * Automatically {@link TeeProcessor#start()} after {@code subscribersCount}
     * {@link TeeProcessor#subscribe(Subscriber)} are done.
     *
     * <p>As {@link TeeProcessor#subscribe(Subscriber)} relies on running code on the event thread,
     * calling {@link TeeProcessor#start()} after {@code n} {@code subscribe()} calls is not
     * thread safe since {@code start()} may run before all subscribers are effectively registered
     * within the event loop. This method checks how many subscribers are effectively subscribed
     * from within the event loop, avoiding the race condition.</p>
     *
     * @param subscribersCount how many subscribers to wait before {@code start()}ing this
     *                         {@link TeeProcessor}.
     * @return {@code this} {@link TeeProcessor} for chaining other calls.
     * @throws IllegalStateException if {@code startAfterSubscribed()},
     *                               {@link TeeProcessor#subscribe(Subscriber)} or
     *                               {@link TeeProcessor#start()} have already been called.
     */
    @SuppressWarnings("UnusedReturnValue")
    public TeeProcessor<T> startAfterSubscribedBy(int subscribersCount) {
        if (startAfterSubscribers < Integer.MAX_VALUE)
            throw new IllegalStateException("startAfterSubscribed() already called for "+this);
        if (!suspended)
            throw new IllegalStateException("previous start() call on "+this);
        startAfterSubscribers = subscribersCount;
        executor().execute(this::checkStartAfterSubscribed);
        return this;
    }

    /**
     * When a {@link Subscriber} {@link TeeProcessor#subscribe(Subscriber)}s after
     * {@link TeeProcessor#start()} and the history buffer is missing items, log
     * a message at the given {@code level}.
     *
     * @param level the SLF4J verbosity {@link Level}
     * @return {@code this} {@link TeeProcessor}
     */
    @SuppressWarnings("unused")
    public TeeProcessor<T> logOnLostItems(Level level) {
        lostItemLogLevel = level;
        return this;
    }

    /**
     * When a {@link Subscriber} {@link TeeProcessor#subscribe(Subscriber)}s after
     * {@link TeeProcessor#start()} and the history buffer is missing items, terminate
     * the subscriber with an {@link MissingHistoryException} to its
     * {@link Subscriber#onError(Throwable)} method.
     *
     * @return {@code this} {@link TeeProcessor}
     */
    @SuppressWarnings("UnusedReturnValue")
    public TeeProcessor<T> errorOnLostItems() {
        lostItemLogLevel = null;
        return this;
    }

    /* --- --- --- subscriber --- --- --- */

    @Override public void onSubscribe(Subscription s) {
        if (upstream == null) upstream = s;
        else                  assert false : "more than one call to onSubscribe()";
    }

    @Override public void onNext(T t) {
        --upstreamRequested;
        history.add(t);
        broadcast();
    }

    @Override public void onError(Throwable t) {
        this.error = t;
        broadcast();
    }

    @Override public void onComplete() {
        this.complete = true;
        broadcast();
    }

    /* --- --- --- publisher --- --- --- */

    @Override public void subscribe(Subscriber<? super T> s) {
        DownstreamState state = new DownstreamState(s);
        executor().execute(() -> doSubscribe(state));
        s.onSubscribe(state);
    }

    /* --- --- --- internal --- --- --- */

    private void subscribeUpstream() {
        assert !suspended;
        if (upstream == null) {
            source.subscribe(this);
            if (suspendedUpstreamRequests > 0) {
                upstreamRequested += suspendedUpstreamRequests;
                upstream.request(suspendedUpstreamRequests);
                suspendedUpstreamRequests = 0;
            }
        }
    }

    private void doStart() {
        suspended = false;
        if (suspendedSubscribe)
            subscribeUpstream();
    }

    private void checkStartAfterSubscribed() {
        if (suspended && downstream.size() >= startAfterSubscribers)
            doStart();
    }

    private void doSubscribe(DownstreamState s) {
        downstream.add(s);
        if (suspended) {
            suspendedSubscribe = true;
            checkStartAfterSubscribed();
            return;
        }
        if (minConsumed > 0) {
            Subscriber<? super T> sub = s.subscriber;
            if (lostItemLogLevel == null) {
                downstream.remove(s);
                s.notifiedTermination = true;
                sub.onError(new MissingHistoryException(this, sub, minConsumed));
                return;
            } else {
                String tpl = "{} lost {} items when subscribing to started {}";
                switch (lostItemLogLevel) {
                    case ERROR:
                        log.error(tpl, sub, minConsumed, this); break;
                    case WARN:
                        log. warn(tpl, sub, minConsumed, this); break;
                    case INFO:
                        log. info(tpl, sub, minConsumed, this); break;
                    case DEBUG:
                        log.debug(tpl, sub, minConsumed, this); break;
                    case TRACE:
                        log.trace(tpl, sub, minConsumed, this); break;
                }
                s.consumed = minConsumed;
            }
        }
        subscribeUpstream();
        s.pull(); // pull events previously delivered
    }

    private void trimHistory() {
        if (downstream.isEmpty())
            return;
        long newMinConsumed = Long.MAX_VALUE;
        for (DownstreamState s : downstream)
            newMinConsumed = Math.min(newMinConsumed, s.consumed);
        if (newMinConsumed > minConsumed) {
            int offset = (int)(newMinConsumed - minConsumed);
            assert history.size() >= offset : "history starting after minConsumed";
            history.removeFirst(offset);
            minConsumed = newMinConsumed;
        }
    }

    private void broadcast() {
        for (DownstreamState s : downstream)
            s.pull();
        try {
            trimHistory();
        } catch (Throwable t) {
            log.error("Unexpected error while trimming history buffer for {}", this, t);
            onError(t);
        }
    }

    @Override public String toString() {
        return "TeeProcessor["+source+"]{"
                + (complete ? "complete" : "!complete")
                + (error == null ? ", !error" : ", error="+error.getClass().getSimpleName())
                + ", minConsumed="+minConsumed
                + ", upstreamRequested="+upstreamRequested
                + ", history="+history + '}';
    }

    private final class DownstreamState implements Subscription {
        private final Subscriber<? super T> subscriber;
        private long consumed = 0, requested = 0;
        private boolean notifiedTermination;

        /* --- --- --- private interface --- --- --- */

        private DownstreamState(Subscriber<? super T> subscriber) {
            this.subscriber = subscriber;
        }

        private void pull() {
            if (notifiedTermination)
                return;
            try {
                long lIdx = consumed - minConsumed;
                if (lIdx < 0)
                    throw new IllegalStateException(this + ".pull(): consumed < minConsumed=" + minConsumed);
                assert lIdx < Integer.MAX_VALUE;
                int idx = (int) lIdx;
                while (requested > 0 && idx < history.size()) {
                    T item = history.get(idx);
                    try {
                        subscriber.onNext(item);
                    } catch (Throwable t) {
                        log.error("downstream={} threw \"{}\" during onNext({}), treating as cancel()",
                                subscriber, t, item);
                        cancel();
                        return;
                    }
                    --requested;
                    ++consumed;
                    ++idx;
                }
                if (idx == history.size())
                    terminate(error);
            } catch (Throwable t) {
                terminate(t);
            }
        }

        private void terminate(@Nullable Throwable error) {
            if (notifiedTermination) return;
            if (error != null) {
                subscriber.onError(error);
                notifiedTermination = true;
            } else if (complete) {
                subscriber.onComplete();
                notifiedTermination = true;
            }
        }

        /* --- --- --- public interface --- --- --- */

        @Override public void request(long n) {
            executor().execute(() -> {
                requested = Math.max(requested, requested+n);
                long inHistory = history.size() - (consumed - minConsumed);
                long mustRequest = requested - inHistory - upstreamRequested;
                if (mustRequest > 0) {
                    if (suspended) {
                        suspendedUpstreamRequests = Math.max(suspendedUpstreamRequests,
                                                             suspendedUpstreamRequests+n);
                    } else {
                        try {
                            upstream.request(mustRequest);
                            upstreamRequested += mustRequest;
                        } catch (Throwable t) {
                            onError(t);
                        }
                    }
                }
                pull();
            });
        }

        @Override public void cancel() {
            executor().execute(() -> {
                try {
                    downstream.remove(this);
                    trimHistory();
                } catch (Throwable t) {
                    log.error("Unexpected exception on {}.cancel()", this, t);
                }
            });
        }

        @Override public String toString() {
            return TeeProcessor.this + ".DownstreamState{" +
                    (notifiedTermination ? "terminated" : "!terminated") +
                    ", consumed=" + consumed +
                    ", requested=" + requested +
                    ", subscriber=" + subscriber + '}';
        }
    }
}
