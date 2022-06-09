package com.github.alexishuf.fastersparql.client.util.reactive;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.alexishuf.fastersparql.client.util.reactive.CallbackPublisher.State.*;

public abstract class CallbackPublisher<T> implements FSPublisher<T> {
    private static final Logger log = LoggerFactory.getLogger(CallbackPublisher.class);
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private static final int NEXT_BATCH   = 16;
    private static final int DEF_CAPACITY = 512;
    private static final int YIELD_MASK   = 128-1;

    /* --- --- --- immutable state --- --- --- */
    private final String name;
    private final ArrayDeque<T> items = new ArrayDeque<>(DEF_CAPACITY);

    /* --- --- --- state changed only from the public interface --- --- --- */
    private Executor executor;
    private BoundedEventLoopPool.LoopExecutor loopExecutor;
    private @MonotonicNonNull Subscriber<? super T> subscriber;
    private Throwable error;
    private boolean feedAfterCompleteWarned;

    /* --- --- --- state changed only from the executor --- --- --- */
    private @NonNegative long requested, nextRequest;
    private volatile boolean subscriberReceivedTerminate = false;
    private final AtomicReference<Thread> eventThread = new AtomicReference<>();

    /* --- --- --- state changed from both the public interface and the executor --- --- --- */
    /*        executor sets DELIVERED and false, pub interface sets PENDING and true         */
    private long pendingRequest;
    private State cancel = NONE;
    private State backpressure = NONE;
    private State termination = NONE;
    private boolean workerActive = false;

    /* --- --- --- constructors --- --- --- */

    public CallbackPublisher(String name, @Nullable Executor executor) {
        this.name = name == null ? "CallbackPublisher-"+nextId.getAndIncrement() : name;
        moveTo(executor == null ? BoundedEventLoopPool.get().chooseExecutor() : executor);
    }

    public CallbackPublisher(String name) {
        this(name, BoundedEventLoopPool.get().chooseExecutor());
    }

    /* --- --- --- public interface --- --- --- */

    @Override public void moveTo(Executor executor) {
        if (executor == null)
            throw new NullPointerException("cannot move to a executor=null");
        if (subscriber != null)
            throw new IllegalStateException("cannot moveTo("+executor+") after subscribed");
        Thread observed = eventThread.get();
        if (observed != null) {
            boolean inEvThread = Thread.currentThread().equals(observed);
            String action;
            if (inEvThread) {
                action = "Running in event thread, cannot wait myself. Will set the executor " +
                        "for future events, but this may cause races.";
            } else {
                action = "Will busy-wait until event thread stops before changing executor";
            }
            log.error("{}.moveTo({}): events already executed, eventThread={}. {}",
                      this, executor, observed, action);
            // if under test, blow up
            assert false : "moveTo() after waking event thread on old executor";
            if (!inEvThread) { // if not under test, try to safely change executor
                //noinspection IdempotentLoopBody
                for (boolean active = true; active; )
                    synchronized (this) { active = workerActive; } // memory barrier
            }
        }
        this.executor = executor;
        this.loopExecutor = executor instanceof BoundedEventLoopPool.LoopExecutor
                          ? (BoundedEventLoopPool.LoopExecutor) executor : null;
    }

    @Override public Executor executor() {
        return executor;
    }

    public void silenceFeedAfterCompleteWarnings() {
        feedAfterCompleteWarned = true;
    }

    public void feed(T item) {
        boolean completed, cancelled, yield = false, wake = false, backpressure = false;
        if (Thread.currentThread().equals(eventThread.get()) && requested > 0
                && subscriber != null && termination != DELIVERED && cancel != DELIVERED ) {
            --requested;
            log.trace("{}.feed({}): directly calling subscriber.onNext()", this, item);
            subscriber.onNext(item);
            return;
        }
        synchronized (this) {
            completed = termination != NONE;
            cancelled = cancel != NONE;
            if (!completed && !cancelled) {
                wake = mustWake();
                items.add(item);
                if (requested == 0 && pendingRequest == 0 && this.backpressure == NONE) {
                    backpressure = true;
                    this.backpressure = PENDING;
                    yield = true;
                } else {
                    yield = (items.size() & YIELD_MASK) == 0;
                }
            }
        }
        if (completed) {
            if (!feedAfterCompleteWarned) {
                feedAfterCompleteWarned = true;
                log.warn("Ignoring {}.feed({}) after complete({})", this, item, errorString());
            }
        } else if (cancelled) {
            log.trace("{}.feed({}): cancel() pending, dropping item", this, item);
        } else {
            log.trace("{}.feed({}): wake={}, yield={}, backpressure={}",
                      this, item, wake, yield, backpressure);
            if (wake)
                executor.execute(spin);
            if (yield)
                Thread.yield();
        }
    }

    public void complete(@Nullable Throwable error) {
        boolean completed, cancelled, wake = false;
        synchronized (this) {
            cancelled = cancel == DELIVERED;
            completed = termination != NONE;
            if (!completed && !cancelled) {
                wake = mustWake();
                termination = State.PENDING;
                this.error = error;
            }
        }
        if (cancelled)
            log.debug("Ignored {}.complete({}) cancel() committed", this, Objects.toString(error));
        else if (completed)
            log.warn("Ignored {}.complete({}) previous complete({})", this, error, errorString());
        else {
            log.trace("{}.complete({}), wake={}", this, error, wake);
            if (wake)
                executor.execute(spin);
        }
    }

    protected abstract void onRequest(long n);
    protected abstract void onBackpressure();
    protected abstract void onCancel();

    @SuppressWarnings("BooleanMethodIsAlwaysInverted") protected boolean isSubscribed() {
        return subscriber != null;
    }


    @Override public void subscribe(Subscriber<? super T> s) {
        if (this.subscriber != null) {
            s.onSubscribe(new Subscription() {
                @Override public void request(long n) {
                    log.error("Invalid subscription to {}", name);
                }
                @Override public void cancel() {
                    log.error("Invalid subscription to {}", name);
                }
            });
            IllegalStateException ex =
                    new IllegalStateException(this + " already subscribed by " + this.subscriber);
            executor.execute(() -> s.onError(ex));
        } else {
            this.subscriber = s;
            log.trace("{}.subscribe({})", this, s);
            s.onSubscribe(new Subscription() {
                @Override public void request(long n) {
                    if (n < 0) {
                        log.error("{}.request({}): negative n", name, n);
                        complete(new IllegalArgumentException("request("+n+"): expected >= 0"));
                        return;
                    }
                    boolean cancelled, terminated, wake = false;
                    synchronized (CallbackPublisher.this) {
                        terminated = termination == DELIVERED;
                        cancelled = cancel != NONE;
                        if (!cancelled && !terminated) {
                            wake = mustWake();
                            pendingRequest += n;
                            backpressure = NONE;
                        }
                    }
                    if (cancelled) {
                        log.warn("Ignoring {}.request({}): previously cancel()ed", name, n);
                    } else if (terminated) {
                        if (subscriberReceivedTerminate) {
                            log.info("Ignoring {}.request({}): after returned {}({})",
                                    name, n, error == null ? "onComplete" : "onError", errorString());
                        } else {
                            log.trace("Ignoring {}.request({}) after complete({})",
                                      name, n, errorString());
                        }
                    } else {
                        log.trace("{}.request({}), wake={}", name, n, wake);
                        if (wake)
                            executor.execute(spin);
                    }
                }
                @Override public void cancel() {
                    boolean cancelled, terminated, wake = false;
                    synchronized (CallbackPublisher.this) {
                        cancelled = cancel != NONE;
                        terminated = termination != NONE;
                        if (!terminated && !cancelled) {
                            wake = mustWake();
                            cancel = PENDING;
                        }
                    }
                    if (cancelled) {
                        log.trace("Ignoring {}.cancel(): prev cancel()", name);
                    } else if (terminated && subscriberReceivedTerminate) {
                        log.trace("Ignoring {}.cancel(): prev complete({})", name, errorString());
                    } else {
                        log.trace("{}.cancel(), wake={}", name, wake);
                        if (wake)
                            executor.execute(spin);
                    }
                }
            });
        }
    }

    @Override public String toString() {
        return name;
    }

    /* --- --- --- implementation details --- --- --- */

    private String errorString() { return Objects.toString(error); }

    enum State {
        NONE,
        PENDING,
        DELIVERED
    }

    private boolean executorFree() {
        return loopExecutor != null && loopExecutor.isFree();
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private enum Action {
        REQUEST {
            @Override public <U> void execute(CallbackPublisher<U> pub) {
                pub.onRequest(pub.nextRequest);
            }
        },
        CANCEL {
            @Override public <U> void execute(CallbackPublisher<U> pub) {
                pub.onCancel();
            }
        },
        NEXT {
            @Override public <U> void execute(CallbackPublisher<U> pub) {
                do {
                    for (int i = 0; i < NEXT_BATCH; i++) {
                        U item;
                        synchronized (pub) {
                            item = pub.requested == 0 ? null : pub.items.poll();
                            if (item != null)
                                --pub.requested;
                        }
                        if (item != null) pub.subscriber.onNext(item);
                        else              return;
                    }
                } while (pub.executorFree());
            }
        },
        BACKPRESSURE {
            @Override public <U> void execute(CallbackPublisher<U> pub) {
                pub.onBackpressure();
            }
        },
        TERMINATE {
            @Override public <U> void execute(CallbackPublisher<U> pub) {
                if (pub.error == null) pub.subscriber.onComplete();
                else                   pub.subscriber.onError(pub.error);
                pub.subscriberReceivedTerminate = true;
            }
        };

        public abstract <U> void execute(CallbackPublisher<U> pub);
    }

    private synchronized Action nextAction() {
        Action action = null;
        if (termination != DELIVERED && subscriber != null) {
            if (cancel == PENDING) {
                cancel = DELIVERED;
                action = Action.CANCEL;
            } else if (cancel == NONE) {
                if (backpressure == PENDING) {
                    backpressure = DELIVERED;
                    action = Action.BACKPRESSURE;
                } else if (pendingRequest > 0) {
                    requested += nextRequest = pendingRequest;
                    pendingRequest = 0;
                    backpressure = NONE;
                    action = Action.REQUEST;
                }
            }
            if (action == null) {
                boolean hasItems = !items.isEmpty();
                if (requested > 0 && hasItems) {
                    action = Action.NEXT;
                } else if (termination == PENDING && !hasItems) {
                    termination = DELIVERED;
                    action = Action.TERMINATE;
                }
            }
        }
        if (action == null)
            workerActive = false;
        return action;
    }

    private final Runnable spin = () -> {
        Thread me = Thread.currentThread();
        if (!eventThread.compareAndSet(null, me) && !me.equals(eventThread.get())) {
            log.error("{}: Event thread changed, was {}. This is a bug.", this, eventThread.get());
            eventThread.set(me);
            assert me.equals(eventThread.get()) : "Changed event thread"; // blow up under test
        }
//        log.trace("{}: spinning from {}", this, workerThread.getName());
        int iterations = 0;
        for (Action a = nextAction(); a != null; a = nextAction(), ++iterations) {
            try {
                a.execute(this);
            } catch (Throwable t) {
                String name = t.getClass().getSimpleName();
                log.error("Unexpected {} executing {} on {}", name, a, this, t);
            }
        }
//        log.trace("{}: leaving spin after {} iterations", this, iterations);
    };

    private boolean mustWake() {
        boolean old = this.workerActive;
        if (!old) this.workerActive = true;
        return !old;
    }
}
