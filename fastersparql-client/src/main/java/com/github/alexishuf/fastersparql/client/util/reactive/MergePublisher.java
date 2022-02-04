package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.util.async.Async;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link Publisher} that emits items from multiple upstream {@link Publisher}s.
 *
 * @param <T> the produced item type
 */
public class MergePublisher<T> implements Publisher<T> {
    private static final Logger log = LoggerFactory.getLogger(MergePublisher.class);
    private final boolean eager, asyncRequest, ignoreUpstreamErrors;

    /**
     * Create a new {@link MergePublisher}
     *
     * @param eager if {@code true}, when there is a request of {@code n} items from downstream
     *              (or from an upstream publisher that terminated before fulfilling its assigned
     *              requests) the first non-terminated upstream publisher will have all of them
     *              allocated. If {@code false}, will try to divide the requests between all
     *              non-terminated upstream publishers.
     * @param asyncRequest if {@code true}, {@link Subscription#request(long)} for upstream
     *                     publishers will be called from a dedicated thread. This ensures
     *                     that publishers which block inside {@code request} do not end up
     *                     blocking other publishers. If the goal is to have parallel consumption,
     *                     this should be {@code true} and {@code eager} should be {@code false}.
     *                     If this is {@code true} and {@code request} for upstream publishers
     *                     does not block, the dedicated threads will be returned to a pool
     *                     when they have no work, allowing their reuse by other tasks.
     * @param ignoreUpstreamErrors if {@code true}, when an upstream publisher delivers an
     *                             {@link Subscriber#onError(Throwable)} event, will interpret the
     *                             event as a {@link Subscriber#onComplete()}. If {@code false},
     *                             any upstream error termination will cause the
     *                             {@link MergePublisher} to terminate with an error event as well.
     */
    public MergePublisher(boolean eager, boolean asyncRequest, boolean ignoreUpstreamErrors) {
        this.eager = eager;
        this.asyncRequest = asyncRequest;
        this.ignoreUpstreamErrors = ignoreUpstreamErrors;
    }

    public static <U> MergePublisher<U> async() {
        return new MergePublisher<>(false, true, false);
    }

    public static <U> MergePublisher<U> eager() {
        return new MergePublisher<>(true, false, false);
    }

    private final ReactiveEventQueue<T> queue = new ReactiveEventQueue<T>() {
        @Override protected void onRequest(long n) {
            distributeRequests(n);
            MergePublisher.this.onRequest(n);
        }
        @Override protected void onTerminate(Throwable cause, boolean cancel) {
            List<Source> copy;
            synchronized (MergePublisher.this) {
                copy = new ArrayList<>(sources);
                sources.clear();
            }
            for (Source source : copy)
                source.cancel();
            MergePublisher.this.onTerminate(cause, cancel);
        }
    };
    protected long undistributedRequests;
    private boolean canComplete;
    private final ArrayList<Source> sources = new ArrayList<>();

    /**
     * Adds a {@link Publisher} for concurrent consumption and delivery of items to
     * the subscriber of this {@link MergePublisher}.
     *
     * @param publisher the publisher to subscribe to
     */
    public void addPublisher(Publisher<? extends T> publisher) {
        Source source = new Source(publisher);
        synchronized (this) {
            log.trace("{}.addPublisher({})", this, publisher);
            sources.add(source);
            source.id = sources.size();
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

    /* --- --- --- implementation details --- --- --- */

    protected void distributeRequests(long additionalRequest) {
        int sizeHint = this.sources.size() + 8;
        ArrayList<Source> copy = new ArrayList<>(sizeHint);
        long[] chunks = new long[sizeHint];
        long rejected = 0;
        do {
            synchronized (this) {
                log.trace("distributeRequests({}), undistributed={}, rejected={}",
                          additionalRequest, undistributedRequests, rejected);
                undistributedRequests += additionalRequest + rejected;
                if (queue.terminated() || undistributedRequests == 0 || sources.isEmpty())
                    return;
                additionalRequest = 0;
                copy.clear();
                int size = sources.size();
                copy.ensureCapacity(size);
                if (chunks.length < size)
                    chunks = Arrays.copyOf(chunks, size);
                for (int i = 0; i < size; i++) {
                    copy.add(sources.get(i));
                    chunks[i] = takeRequests();
                }
            }
            rejected = 0;
            for (int i = 0, size = copy.size(); i < size; i++) {
                if (!copy.get(i).tryRequest(chunks[i]))
                    rejected += chunks[i];
            }
        } while (rejected > 0);
    }

    protected synchronized long takeRequests() {
        long chunk = eager ? undistributedRequests : undistributedRequests / sources.size();
        if (chunk == 0)
            chunk = undistributedRequests;
        undistributedRequests -= chunk;
        return chunk;
    }

    private final class Source implements Subscriber<T> {
        private final Publisher<? extends T> publisher;
        private @MonotonicNonNull Subscription upstream;
        private long requested = 0, futureRequest = 0;
        private int id = -1;
        private boolean terminated, subscribed, requestThreadAlive;

        public Source(Publisher<? extends T> publisher) {
            this.publisher = publisher;
        }

        @Override public synchronized void onSubscribe(Subscription s) {
            assert upstream == null;
            upstream = s;
            if (terminated)
                s.cancel();
        }

        public void cancel() {
            boolean call = false;
            long unsatisfied = 0;
            synchronized (this) {
                if (!terminated) {
                    log.trace("Source {} cancel()ed", id);
                    terminated = true;
                    call = upstream != null && requested == 0;
                    unsatisfied = requested + futureRequest;
                    requested = futureRequest = 0;
                }
            }
            if (call)
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
            log.trace("Source {}: tryRequest({})", id, n);
            assert n >= 0 : "negative tryRequest()";
            if (n <= 0)
                return true;
            boolean call;
            synchronized (this) {

                if (terminated) {
                    return false;
                }
                if (!subscribed) {
                    subscribed = true;
                    publisher.subscribe(this);
                }
                if (asyncRequest) {
                    log.trace("Source {}: tryRequest({}) terminated={}, requestThreadAlive={}",
                              id, n, terminated, requestThreadAlive);
                    call = false;
                    futureRequest += n;
                    if (!requestThreadAlive) {
                        requestThreadAlive = true;
                        Async.async(this::requestThread);
                    }
                } else {
                    call = requested == 0;
                    if (call) requested = n;
                    else      futureRequest += n;
                    log.trace("Source {}: tryRequest({}) call={}", id, n, call);
                }
            }
            if (call)
                upstream.request(requested);

            return true;
        }

        private void requestThread() {
            assert requestThreadAlive;
            while (true) {
                long n;
                synchronized (this) {
                    log.trace("requestThread() for Source {}: requested={}, futureRequest={}, terminated={}",
                              id, requested, futureRequest, terminated);
                    requested += n = futureRequest;
                    futureRequest = 0;
                    if (n == 0 || terminated) {
                        requestThreadAlive = false;
                        break;
                    }
                }
                upstream.request(n);
            }
        }

        @Override public void onNext(T item) {
            log.trace("Source {}: onNext({})", id, item);
            assert upstream != null && subscribed;
            long requestSize;
            synchronized (this) {
                requested = Math.max(0, requested - 1 + (requestSize = futureRequest));
                futureRequest = 0;
                if (requested == 0 && requestSize == 0 && !terminated)
                    requested = requestSize = takeRequests();
                assert !terminated || requestSize == 0 : "terminated==true with futureRequest > 0";
            }
            if (terminated) {
                upstream.cancel();
            } else {
                queue.send(item).flush();
                if (requestSize > 0)
                    upstream.request(requestSize);

            }
        }

        @Override public void onError(Throwable t) {
            if (ignoreUpstreamErrors) {
                log.debug("Treating {} from {} as a normal complete", t, publisher);
                onComplete();
            } else {
                log.trace("Source {} received error from upstream", id, t);
                synchronized (MergePublisher.this) {
                    sources.remove(this);
                }
                synchronized (this) {
                    terminated = true;
                }

                queue.sendComplete(t).flush();
            }
        }

        @Override public void onComplete() {
            long unsatisfied;
            boolean complete;
            synchronized (MergePublisher.this) {
                synchronized (this) {
                    terminated = true;
                    unsatisfied = requested + futureRequest;
                    requested = futureRequest = 0;
                    sources.remove(this);
                    complete = canComplete && sources.isEmpty();
                }
            }
            if (complete) {
                log.trace("Source {} completed causing MergePublisher completion", id);
                queue.sendComplete(null).flush();
            } else if (unsatisfied > 0) {
                log.trace("Source {} completed with {} unsatisfied requests", id, unsatisfied);
                distributeRequests(unsatisfied);
            }
        }
    }
}
