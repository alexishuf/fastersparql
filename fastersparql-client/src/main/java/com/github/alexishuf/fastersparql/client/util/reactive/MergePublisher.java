package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.util.async.Async;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.max;
import static java.lang.Thread.currentThread;

/**
 * A {@link Publisher} that emits items from multiple upstream {@link Publisher}s.
 *
 * @param <T> the produced item type
 */
public class MergePublisher<T> implements Publisher<T> {
    private static final Logger log = LoggerFactory.getLogger(MergePublisher.class);
    private static final AtomicInteger nextAnonId = new AtomicInteger(1);

    private final String name;
    private final boolean ignoreUpstreamErrors;
    private int parallelism = -1;
    private final ReactiveEventQueue<T> queue;
    protected long undistributedRequests, sourcesGen = 0;
    private boolean canComplete;
    private int nextSourceId = 1;
    private final ArrayList<Source> sources = new ArrayList<>();

    /**
     * Create a new {@link MergePublisher}
     *
     * @param ignoreUpstreamErrors if {@code true}, when an upstream publisher delivers an
     *                             {@link Subscriber#onError(Throwable)} event, will interpret the
     *                             event as a {@link Subscriber#onComplete()}. If {@code false},
     *                             any upstream error termination will cause the
     *                             {@link MergePublisher} to terminate with an error event as well.
     */
    public MergePublisher(@Nullable String name,
                          boolean ignoreUpstreamErrors) {
        this.name = name != null ? name : "MergePublisher-"+nextAnonId.getAndIncrement();
        this.ignoreUpstreamErrors = ignoreUpstreamErrors;
        this.queue = new ReactiveEventQueue<T>(this.name) {
            @Override protected void onRequest(long n) {
                distributeRequests(n);
                MergePublisher.this.onRequest(n);
            }
            @Override protected void onTerminate(Throwable cause, boolean cancel) {
                List<Source> copy;
                synchronized (MergePublisher.this) {
                    copy = new ArrayList<>(sources);
                    sources.clear();
                    ++sourcesGen;
                }
                for (Source source : copy)
                    source.cancel();
                MergePublisher.this.onTerminate(cause, cancel);
            }
        };
    }

    public static <U> MergePublisher<U> async(@Nullable String name) {
        return new MergePublisher<>(name, false);
    }

    public static <U> MergePublisher<U> eager(@Nullable String name) {
        return new MergePublisher<U>(name, false).setTargetParallelism(1);
    }

    /**
     * Sets the target for parallel consumption of sources.
     *
     * Let {@code req} be the accumulated number of items requested via
     * {@link Subscription#request(long)} from this {@link MergePublisher} not yet delivered
     * via {@link Subscriber#onNext(Object)} by this {@link MergePublisher}. If the number of
     * sources is less than {@code n}, each source will nevertheless receive a request
     * for {@code req/n} (or 1 if that division yields zero). If the number of sources is
     * {@code m > n} then each source will receive a request for {@code req/m},
     *
     * If this method is not called, the number of sources will be used when partitioning work.
     *
     * This method SHOULD be called if the intent is to distribute work amoung multiple sources.
     * With the default behavior, no matter how many sources are added, the first source to be
     * added will receive all the accumulated requests for items, leaving sources added
     * microseconds later stalled.
     *
     * @param n the number of sources to consider when distributing work
     * @return this {@link MergePublisher}
     */
    public MergePublisher<T> setTargetParallelism(int n) {
        parallelism = n;
        return this;
    }


    /**
     * Adds a {@link Publisher} for concurrent consumption and delivery of items to
     * the subscriber of this {@link MergePublisher}.
     *
     * @param publisher the publisher to subscribe to
     */
    public void addPublisher(Publisher<? extends T> publisher) {
        synchronized (this) {
            Source source = new Source(publisher, nextSourceId++);
            log.trace("{}.addPublisher({})", this, publisher);
            sources.add(source);
            ++sourcesGen;
        }
        if (queue.subscriber() != null)
            distributeRequests(0);
    }

    /**
     * After this method is called, when there are zero active publishers, the
     * {@link MergePublisher} itself will complete calling {@link Subscriber#onComplete()}
     */
    public void markCompletable() {
        boolean complete;
        synchronized (this) {
            if (canComplete)
                return;
            canComplete = true;
            log.trace("{}.markComplete()", this);
            complete = sources.isEmpty();
            if (complete)
                queue.sendComplete(null);
        }
        if (complete)
            queue.flush();
    }

    /**
     * This will be called for every {@link Subscription#request(long)} made by the
     * downstream subscriber After all the request has been registered within the
     * {@link MergePublisher}.
     *
     * @param n the number of additional items requested
     */
    protected void onRequest(long n) {
        /* do nothing */
    }

    /**
     * This will be called after the {@link MergePublisher} enter the terminated state. This
     * only occurs once in the lifetime of the {@link Publisher}.
     *
     * Note: If {@code cancelled = false}, the downstream {@link Subscriber} methods have not yet
     * been called and will be called after this method call returns.
     *
     * @param cause If non-null, this is the error that caused termination.
     * @param cancelled if true, the downstream subscriber either called {@code cancel()}
     *                  or threw from its {@code onNext(T)}
     */
    protected void onTerminate(Throwable cause, boolean cancelled) {
        /* do nothing */
    }

    @Override public void subscribe(Subscriber<? super T> s) {
        log.trace("{}.subscribe({})", this, s);
        queue.subscribe(s);
    }

    @Override public String toString() {
        return name;
    }

    /* --- --- --- implementation details --- --- --- */

    private String dumpIfTrace() {
        if (!log.isTraceEnabled()) return "";
        StringBuilder b = new StringBuilder();
        synchronized (this) {
            b.append("undistributed=").append(undistributedRequests);
            b.append(queue.terminated() ? ", " : ", !").append("terminated");
            b.append(" sources=[");
            for (Source s : sources) {
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (s) {
                    b.append(s.id).append(s.terminated ? "{" : "{!").append("term")
                            .append(", sub=").append(s.subscribedStage)
                            .append(", req=").append(s.requested)
                            .append(", futReq=").append(s.futureRequest)
                            .append("}, ");
                }
            }
            if (!sources.isEmpty())
                b.setLength(b.length()-2);
        }
        return b.append(']').toString();
    }

    protected void distributeRequests(long additionalRequests) {
        ArrayList<Source> copy = new ArrayList<>(sources.size());
        long taken, chunk, distributed, redistribute = 0, lastGen = sourcesGen-1;
        while (true) {
            synchronized (this) {
                this.undistributedRequests += additionalRequests + redistribute;
                if (this.undistributedRequests == 0 || sources.isEmpty() || lastGen == sourcesGen) {
                    log.trace("{}.distributeRequests({}) leaving redistribute={} lastGen={} {}",
                              this, additionalRequests, redistribute, lastGen, dumpIfTrace());
                    return;
                } else {
                    log.trace("{}.distributeRequests({}) redistribute={} lastGen={} {}",
                              this, additionalRequests, redistribute, lastGen, dumpIfTrace());
                }
                additionalRequests = redistribute = distributed = 0;
                copy.clear();
                copy.addAll(sources);
                lastGen = sourcesGen;
                long div = parallelism > 0 ? parallelism : Math.max(1, copy.size());
                chunk = this.undistributedRequests / div;
                if (chunk == 0) {
                    this.undistributedRequests -= chunk = taken = this.undistributedRequests;
                } else {
                    taken = Math.min(this.undistributedRequests, chunk * copy.size());
                    this.undistributedRequests -= taken;
                }
                assert chunk*copy.size() >= taken;
            }
            for (int i = 0, size = copy.size(); distributed+redistribute < taken && i < size; i++) {
                if (copy.get(i).tryRequest(chunk)) distributed  += chunk;
                else                               redistribute += chunk;
            }
            assert distributed + redistribute == taken;
        }
    }

    private synchronized long takeChunk() {
        long chunk = undistributedRequests
                   / (parallelism > 0 ? parallelism : sources.size());
        if (chunk == 0) chunk = undistributedRequests;
        undistributedRequests -= chunk;
        log.trace("{}.takeChunk()={} now undistributedRequests={}",
                  this, chunk, undistributedRequests);
        return chunk;
    }

    private static final int ST_NOT_SUBSCRIBED   = 0;
    private static final int ST_SUBSCRIBE_CALLED = 1;
    private static final int ST_SUBSCRIBED       = 2;

    private final class Source implements Subscriber<T> {
        private final Publisher<? extends T> publisher;
        private @MonotonicNonNull Subscription upstream;
        private long requested = 0, futureRequest = 0, declaredFutureRequest;
        private final int id;
        private int subscribedStage = ST_NOT_SUBSCRIBED;
        private boolean terminated, eventThreadAlive, futureFlush;
        private Thread eventThread = null;

        public Source(Publisher<? extends T> publisher, int id) {
            this.publisher = publisher;
            this.id = id;
        }

        @Override public String toString() {
            return name+"[source="+id+"]";
        }

        @Override public void onSubscribe(Subscription s) {
            long n = 0;
            synchronized (this) {
                assert upstream == null;
                assert subscribedStage == ST_SUBSCRIBE_CALLED;
                subscribedStage = ST_SUBSCRIBED;
                upstream = s;
                if (futureRequest > 0) {
                    n = futureRequest;
                    futureRequest = 0;
                }
                if (terminated)
                    s.cancel();
            }
            if (n > 0 && !tryRequest(n))
                distributeRequests(n);
        }

        public void cancel() {
            long unsatisfied = 0;
            synchronized (this) {
                if (!terminated) {
                    log.trace("{} cancel()ed", this);
                    terminated = true;
                    unsatisfied = requested + futureRequest;
                    requested = futureRequest = declaredFutureRequest = 0;
                }
            }
            if (upstream != null)
                upstream.cancel();
            //else: cancel on next onNext() call
            if (unsatisfied > 0)
                distributeRequests(unsatisfied);
        }

        /**
         * Schedules or calls {@code upstream.request(n)}
         * @return true iff request was or will be called. false if terminated or not yet subscribed
         */
        public boolean tryRequest(long n) {
            assert n >= 0 : "negative tryRequest()";
            if (n <= 0)
                return true;
            boolean subscribe = false;
            synchronized (this) {
                if (terminated) {
                    log.trace("{}.tryRequest({}): terminated", this, n);
                    return false;
                } else {
                    futureRequest += n;
                    if (subscribedStage == ST_NOT_SUBSCRIBED) {
                        log.trace("{}.tryRequest({}): subscribing", this, n);
                        subscribedStage = ST_SUBSCRIBE_CALLED;
                        subscribe = true;
                    } else if (subscribedStage == ST_SUBSCRIBE_CALLED) {
                        log.trace("{}.tryRequest({}), waiting onSubscribe", this, n);
                    } else {
                        log.trace("{}.tryRequest({}), will request from eventThread()", this, n);
                        wakeEventThread(false);
                    }
                }
            }
            if (subscribe)
                publisher.subscribe(this);
            return true;
        }

        private void eventThread() {
            assert eventThreadAlive;
            assert eventThread == null;
            eventThread = currentThread();
            while (true) {
                boolean mustFlush;
                long requestSize;
                synchronized (this) {
                    log.trace("{}.eventThread(): requested={}, futureRequest={}, terminated={}",
                            this, requested, futureRequest, terminated);
                    if (upstream == null) {
                        requestSize = 0;
                    } else {
                        requestSize = futureRequest + declaredFutureRequest;
                        requested += futureRequest;
                        futureRequest = declaredFutureRequest = 0;
                    }
                    mustFlush = futureFlush;
                    futureFlush = false;
                    if (requestSize == 0 && !mustFlush) {
                        log.trace("{}.eventThread() leaving: requested={}, futureRequest={}, terminated={}",
                                  this, requested, futureRequest, terminated);
                        eventThreadAlive = false;
                        eventThread = null;
                        break;
                    }
                }
                try {
                    if (mustFlush)
                        queue.flush();
                    if (requestSize > 0) {
                        upstream.request(requestSize);
                        queue.flush(); // always flush after a request instead of waiting on*()
                    }
                } catch (Throwable t) {
                    log.error("Exception on {}.eventThread()", this, t);
                }
            }
        }

        private void wakeEventThread(boolean flush) {
            synchronized (this) {
                futureFlush |= flush;
                assert futureRequest >= 0;
                assert declaredFutureRequest >= 0;
                assert requested >= 0;
                boolean hasWork = flush || futureRequest > 0 || declaredFutureRequest > 0;
                if (hasWork) {
                    if (!eventThreadAlive) {
                        log.trace("{}.wakeEventThread({}) starting thread", this, flush);
                        eventThreadAlive = true;
                        Async.async(this::eventThread);
                    } else {
                        log.trace("{}.wakeEventThread({}) {}", this, flush,
                                  eventThread == currentThread() ? "in thread" : "thread alive");
                    }
                }
            }
        }

        @Override public void onNext(T item) {
            assert upstream != null && subscribedStage == ST_SUBSCRIBED;
            long takenChunk = 0;
            if (!terminated && requested+futureRequest == 1) {
                takenChunk = takeChunk();
                log.trace("{}.onNext({}) takenChunk={}", this, item, takenChunk);
            } else {
                log.trace("{}.onNext({})", this, item);
            }
            boolean returnTaken;
            synchronized (this) {
                assert terminated || requested > 0 : "onNext() without backing request";
                returnTaken = terminated && takenChunk > 0;
                long requestSize = futureRequest + (returnTaken ? 0 : takenChunk);
                futureRequest = 0;
                requested = max(0, requested - 1 + requestSize);
                declaredFutureRequest += requestSize;
            }
            if (returnTaken) {
                log.trace("{}.onNext({}) returning {} takenChunk", this, item, takenChunk);
                distributeRequests(takenChunk);
            }
            queue.send(item);
            wakeEventThread(true);
        }

        @Override public void onError(Throwable t) {
            log.trace("{}.onError({})", this, t);
            if (ignoreUpstreamErrors) {
                log.debug("Treating {} from {} as a normal complete", t, publisher);
                onComplete();
            } else {
                synchronized (MergePublisher.this) {
                    synchronized (this) {
                        sources.remove(this);
                        ++sourcesGen;
                        terminated = true;
                        undistributedRequests += requested + futureRequest;
                        requested = futureRequest = declaredFutureRequest = 0;
                        log.trace("{}.onError({}): set terminate {}", this, t, dumpIfTrace());
                    }
                }
                queue.sendComplete(t);
                wakeEventThread(true);
            }
        }

        @Override public void onComplete() {
            long unsatisfied;
            boolean complete;
            synchronized (MergePublisher.this) {
                synchronized (this) {
                    terminated = true;
                    undistributedRequests += unsatisfied = requested + futureRequest;
                    requested = futureRequest = declaredFutureRequest = 0;
                    sources.remove(this);
                    ++sourcesGen;
                    complete = canComplete && sources.isEmpty();
                    log.trace("{}.onComplete(): updated state {}", this, dumpIfTrace());
                }
            }
            if (complete) {
                log.trace("{} completed causing MergePublisher completion", this);
                queue.sendComplete(null);
                wakeEventThread(true);
            } else {
                log.trace("{} completed with {} unsatisfied requests", this, unsatisfied);
                distributeRequests(0);
            }
        }
    }
}
