package com.github.alexishuf.fastersparql.client.util.reactive;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Accessors(fluent = true)
public class ReactiveEventLoopSketch {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    @Getter private final String name;
    private final BoundedEventLoopPool.LoopExecutor loopExecutor;
    private boolean workerActive = false;
    private volatile Thread workerThread = null;
    private final ArrayDeque<Event> queue = new ArrayDeque<>(1024);
    private final Runnable spin = () -> {
        workerThread = Thread.currentThread();
        for (Event ev = dequeue(); ev != null; ev = dequeue())
            executeEvent(ev);
    };

    /**
     * Create new event loop with given name bound to a worker from the given pool
     *
     * @param pool thread pool from where events will be serially executed from
     *             a preferred thread. If null will use the global shared instance,
     *             {@link BoundedEventLoopPool#get()}
     * @param name the name of the event loop, used when logging events. If {@code null}
     *             will generate a name using a sequential counter that starts on {@code 1}.
     */
    public ReactiveEventLoopSketch(@Nullable BoundedEventLoopPool pool, @Nullable String name) {
        this.name = name != null ? name : "ReactiveEventLoop-"+nextId.getAndIncrement();
        this.loopExecutor = (pool != null ? pool : BoundedEventLoopPool.get()).chooseExecutor();
    }

    /**
     * Executes {@code p.subscribe(s)} on the event loop thread.
     *
     * This differs from other enqueueing methods in that the subscribe method call is typically
     * quick and nearly all implementations will cal {@link Subscriber#onSubscribe(Subscription)}
     * directly from within the {@link Publisher#subscribe(Subscriber)} method call. In addition,
     * the {@link Subscriber#onSubscribe(Subscription)} event is the first event ever among the
     * events that must be serially delivered (the events cannot be reordered nor concurrently
     * delivered).
     *
     * Thus, if this is called from the event loop thread, instead of queueing,
     * {@code p.subscribe(s)} will be directly called. Else, the event will be queued as the
     * other events are.
     *
     * <strong>Important: </strong> Execution of {@code s.onSubscribed()} and
     * {@code p.subscribe(s)} is guaranteed to have completed by the return if this method call
     * iff {@link ReactiveEventLoopSketch#inLoopThread()} is {@code true}. Only queueing of a future
     * execution of {@code p.subscribe(s)} is guaranteed.
     *
     * @param s the subscriber
     */
    public <T> void subscribe(Publisher<T> p, Subscriber<? super T> s) {
        if (inLoopThread()) p.subscribe(s);
        else                queue(new Subscribe<>(p, s));
    }

    /**
     * Calls {@link Subscriber#onSubscribe(Subscription)} of {@code subscriber} with
     * {@code subscription} from within the event loop thread.
     *
     * Given the caveats noted in {@link ReactiveEventLoopSketch#subscribe(Publisher, Subscriber)},
     * the call to {@code subscriber.onSubscribe(subscription} will be made directly if running
     * the event loop thread or this thread will block until the event is processed by the
     * event loop thread.
     *
     * @param subscriber the {@link Subscriber} to have
     *                   {@link Subscriber#onSubscribe(Subscription)} called
     * @param subscription the {@link Subscription} to pass
     *                     {@link Subscriber#onSubscribe(Subscription)}.
     */
    public void subscribed(Subscriber<?> subscriber, Subscription subscription) {
        if (inLoopThread()) {
            subscriber.onSubscribe(subscription);
        } else {
            queue(new Subscribed<>(subscriber, subscription));
        }
    }

    /**
     * Queues an execution of {@code subscription.request(n)} in the event loop thread.
     */
    public void request(Subscription subscription, long n) {
        queue(new Request(subscription, n));
    }

    /**
     * Queues an execution of {@code subscription.cancel()} in the event loop thread.
     */
    public void cancel(Subscription subscription) {
        queue(new Cancel(subscription));
    }

    /**
     * Queues an execution of {@code subscriber.onNext(item)} in the event loop thread.
     */
    public <T> void next(Subscriber<? super T> subscriber, T item) {
        queue(new Next<>(subscriber, item));
    }

    /**
     * Queues an execution of {@code subscriber.onError(cause)} in the event loop thread.
     */
    public <T> void error(Subscriber<T> subscriber, Throwable cause) {
        queue(new Error<>(subscriber, cause));
    }

    /**
     * Queues an execution of {@code subscriber.onComplete()} in the event loop thread.
     */
    public <T> void complete(Subscriber<T> subscriber) {
        queue(new Complete<>(subscriber));
    }

    /**
     * Tests whether the calling thread is the worker thread for this loop.
     */
    public boolean inLoopThread() {
        return Thread.currentThread().equals(workerThread);
    }

    @Override public String toString() {
        return name;
    }

    /* --- --- --- Bound wrappers (de)constructors  --- --- --- */

    public static <T> Subscriber<T> unbind(Subscriber<T> s) {
        while (s instanceof BoundSubscriber) s = ((BoundSubscriber<T>) s).delegate;
        return s;
    }
    public <T> Subscriber<T> bind(Subscriber<T> subscriber) {
        if (subscriber instanceof BoundSubscriber) {
            BoundSubscriber<T> bound = (BoundSubscriber<T>) subscriber;
            if (bound.loop != this)
                log.warn("Moving Subscriber {} from {} to {}", bound.delegate, bound.loop, this);
            bound.loop = this;
            return bound;
        }
        return new BoundSubscriber<>(this, subscriber);
    }

    public static Subscription unbind(Subscription s) {
        while (s instanceof BoundSubscription) s = ((BoundSubscription) s).delegate;
        return s;
    }
    public Subscription bind(Subscription subscription) {
        if (subscription instanceof BoundSubscription) {
            BoundSubscription bound = (BoundSubscription) subscription;
            if (bound.loop != this)
                log.warn("Moving Subscription {} from {} to {}", bound.delegate, bound.loop, this);
            bound.loop = this;
            return bound;
        }
        return new BoundSubscription(this, subscription);
    }

    public static  <T> Publisher<T> unbind(Publisher<T> p) {
        while (p instanceof BoundPublisher) p = ((BoundPublisher<T>) p).delegate;
        return p;
    }
    public <T> Publisher<T> bind(Publisher<T> publisher) {
        if (publisher instanceof BoundPublisher) {
            BoundPublisher<T> bound = (BoundPublisher<T>) publisher;
            if (bound.loop != this)
                log.trace("Moving Publisher {} from {} to {}", bound.delegate, bound.loop, this);
            bound.loop = this;
            return bound;
        }
        return new BoundPublisher<>(this, publisher);
    }

    /* --- --- --- binding wrappers --- --- --- */

    @AllArgsConstructor @ToString
    public static class BoundSubscriber<T> implements Subscriber<T> {
        protected ReactiveEventLoopSketch loop;
        protected final Subscriber<T> delegate;

        /**
         * Since this is the first event ever, there is no need to deliver previously queued
         * events before delivering it. Thus, if called from within the event loop thread,
         * will directly call {@code delegate.onSubscribe}. If called from another thread,
         * will queue a event and block until that event is processed.
         *
         * @param s the subscription to deliver via {@link Subscriber#onSubscribe(Subscription)}.
         */
        @Override public void onSubscribe(Subscription s) {
            loop.subscribed(delegate, loop.bind(s));
        }
        @Override public void     onNext(T t)         { loop.    next(delegate, t); }
        @Override public void    onError(Throwable t) { loop.   error(delegate, t); }
        @Override public void onComplete()            { loop.complete(delegate);    }
    }

    @AllArgsConstructor @ToString
    public static class BoundSubscription implements Subscription {
        protected ReactiveEventLoopSketch loop;
        protected final Subscription delegate;
        @Override public void request(long n) { loop.request(delegate, n); }
        @Override public void cancel()        { loop. cancel(delegate);    }
    }

    @AllArgsConstructor @ToString
    public static class BoundPublisher<T> implements Publisher<T> {
        protected ReactiveEventLoopSketch loop;
        protected final Publisher<T> delegate;
        @Override public void subscribe(Subscriber<? super T> s) {
            loop.subscribe(delegate, loop.bind(s));
        }
    }

    /* --- --- --- Implementation details --- --- --- */

    private static abstract class Event {
        private static final boolean SAVE_ORIGIN = log.isTraceEnabled();
        protected Throwable origin;

        public Event() {
            if (SAVE_ORIGIN)
                origin = new RuntimeException();
        }

        abstract void execute();
    }

    @RequiredArgsConstructor @ToString
    private static final class Subscribe<T> extends Event {
        private final @lombok.NonNull Publisher<? extends T> p;
        private final @lombok.NonNull Subscriber<? super T> s;
        @Override void execute() { p.subscribe(s); }
    }

    @RequiredArgsConstructor @ToString
    private static final class Subscribed<T> extends Event {
        private final @lombok.NonNull Subscriber<? super T> s;
        private final @lombok.NonNull Subscription subscription;
        @Override void execute() { s.onSubscribe(subscription); }
    }

    @RequiredArgsConstructor @ToString
    private static final class Request extends Event {
        private final @lombok.NonNull Subscription subscription;
        private final long n;
        @Override void execute() { subscription.request(n); }
    }

    @RequiredArgsConstructor @ToString
    private static final class Cancel extends Event {
        private final @lombok.NonNull Subscription subscription;
        @Override void execute() { subscription.cancel(); }
    }

    @RequiredArgsConstructor @ToString
    private static final class Next<T> extends Event {
        private final @lombok.NonNull Subscriber<T> subscriber;
        private final T item;
        @Override void execute() { subscriber.onNext(item); }
    }

    @RequiredArgsConstructor @ToString
    private static final class Complete<T> extends Event {
        private final @lombok.NonNull Subscriber<T> subscriber;
        @Override void execute() { subscriber.onComplete(); }
    }

    @RequiredArgsConstructor @ToString
    private static final class Error<T> extends Event {
        private final @lombok.NonNull Subscriber<T> subscriber;
        private final @lombok.NonNull Throwable cause;
        @Override void execute() { subscriber.onError(cause); }
    }

    private synchronized Event dequeue() {
        Event ev = queue.poll();
        if (ev == null) {
            log.trace("{}.spin() will return, workerActive <- false", this);
            workerThread = null;
            workerActive = false;
        }
        return ev;
    }

    private void executeEvent(Event ev) {
        try {
            log.trace("executing {}", ev);
            ev.execute();
        } catch (Throwable t) {
            if (ev.origin != null) {
                StringBuilder b = new StringBuilder();
                StackTraceElement[] trace = ev.origin.getStackTrace();
                for (int i = 3; i < trace.length; i++) {
                    StackTraceElement element = trace[i];
                    b.append("    ").append(element).append('\n');
                }
                b.setLength(Math.max(0, b.length()-2));
                String tName = t.getClass().getSimpleName();
                log.error("Unexpected {}} from {}, queued at\n{}", tName, ev, b, t);
            } else {
                log.error("Unexpected exception from {}, queued at ", ev, t);
            }
        }
    }

    private synchronized void queue(Event event) {
        log.trace("queue({}), workerActive={}", event, workerActive);
        queue.add(event);
        if (!workerActive) {
            workerActive = true;
            loopExecutor.execute(spin);
        }
    }
}
