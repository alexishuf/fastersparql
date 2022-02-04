package com.github.alexishuf.fastersparql.client.util.reactive;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j @Accessors(fluent = true)
abstract class ReactiveEventQueue<T> {
    private long requested;
    private @Getter @Setter @MonotonicNonNull Subscriber<? super T> subscriber;
    private @Getter boolean terminated;
    private boolean paused;
    private final AtomicInteger flushing = new AtomicInteger();
    private final Queue<Object> queue = new ConcurrentLinkedDeque<>();

    public ReactiveEventQueue<T> request(long n) {
        synchronized (this) {
            if (!terminated) {
                if (n < 0) {
                    sendComplete(new IllegalArgumentException("request(" + n + "), expected > 0"));
                } else if (n > 0) {
                    requested += n;
                    if (paused) {
                        paused = false;
                        resume();
                    }
                }
            }
        }
        if (n > 0) onRequest(n);
        return this;
    }

    public ReactiveEventQueue<T> send(T o) {
        log.trace("send({})", o);
        queue.add(o);
        return this;
    }

    public ReactiveEventQueue<T> sendComplete(@Nullable Throwable error) {
        log.trace("sendComplete({}{})", "", error);
        queue.add(new TerminateMessage(this, error));
        return this;
    }

    public void cancel() {
        synchronized (this) {
            terminated = true;
            requested = 0;
            queue.clear();
        }
        onTerminate(null, true);
    }

    protected void pause() {
        log.trace("pause()");
        /* no op */
    }

    protected void resume() {
        log.trace("resume()");
        /* no op */
    }

    protected abstract void onRequest(long n);

    protected void onTerminate(Throwable cause, boolean cancel) {
        log.trace("onTerminate({}, {})", cause, cancel);
        /* no op */
    }

    public void subscribe(Subscriber<? super T> s) {
        if (subscriber != null) {
            s.onSubscribe(new Subscription() {
                @Override public void request(long n) { }
                @Override public void cancel() { }
            });
            s.onError(new IllegalStateException(this+" only accepts a single subscriber"));
        } else {
            subscriber = s;
            s.onSubscribe(new Subscription() {
                @Override public void request(long n) {
                    log.trace("request({})", n);
                    if (n < 0) {
                        sendComplete(new IllegalArgumentException("request("+n+"), expected > 0"));
                    } else if (!terminated) {
                        ReactiveEventQueue.this.request(n).flush();
                    }
                }
                @Override public void cancel() {
                    ReactiveEventQueue.this.cancel();
                }
            });
            flush();
        }
    }

    /** There is no thread in the flushing */
    private static final int ST_NO_FLUSHER = 0;
    /** There is a thread flushing */
    private static final int ST_FLUSHING = 1;
    /** There is a thread flushing, and it will call dequeueRequested() at least once more */
    private static final int ST_RECHECK = 2;

    public void flush() {
        while (!flushing.compareAndSet(ST_NO_FLUSHER, ST_FLUSHING)) {
            if (flushing.get() == ST_RECHECK || flushing.compareAndSet(ST_FLUSHING, ST_RECHECK))
                return; // we've confirmed the flusher thread will call dequeueRequested()
        }
        //this thread is now the flusher
        try {
            do {
                for (Object obj = dequeueRequested(); obj != null; obj = dequeueRequested()) {
                    if (obj instanceof TerminateMessage) {
                        ((TerminateMessage) obj).execute();
                    } else {
                        try {
                            //noinspection unchecked
                            subscriber.onNext((T) obj);
                        } catch (Throwable t) {
                            log.error("{}.onNext({}) threw {}. Treating as cancelled",
                                      subscriber, obj, t, t);
                            terminated = true;
                            onTerminate(null, true);
                            queue.clear();
                        }
                    }
                }
            } while (flushing.getAndDecrement() > ST_FLUSHING);
        } catch (Throwable t) {
            flushing.set(ST_NO_FLUSHER); // do not die with the lock, causing starvation.
            throw t;
        }
    }

    private synchronized @Nullable Object dequeueRequested() {
        Object o = null;
        if (subscriber == null || terminated) {
            return null;
        } else if (requested > 0) {
            if ((o = queue.poll()) != null)
                --requested;
        } else if (!queue.isEmpty()) {
            if (queue.peek() instanceof TerminateMessage) {
                o = queue.poll();
            } else if (!paused) {
                paused = true;
                pause();
            }
        }
        return o;
    }

    @RequiredArgsConstructor
    private static final class TerminateMessage {
        private final ReactiveEventQueue<?> queue;
        private final @Nullable Throwable error;

        void execute() {
            synchronized (queue) {
                if (queue.terminated)
                    return;
                queue.terminated = true;
            }
            queue.onTerminate(error, false);
            assert queue.subscriber != null : "null subscriber, should not have called me!";
            try {
                if (error != null)
                    queue.subscriber.onError(error);
                else
                    queue.subscriber.onComplete();
            } catch (Throwable t) {
                log.warn("Ignoring {} thrown by {}.{}({})",
                        t, queue.subscriber, error == null ? "onComplete" : "onError", error);
            }
        }
    }
}
