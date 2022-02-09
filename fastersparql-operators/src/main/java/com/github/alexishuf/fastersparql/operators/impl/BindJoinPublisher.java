package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.EmptyPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.MergePublisher;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

class BindJoinPublisher<R> extends MergePublisher<R> {
    public enum JoinType {
        INNER,
        LEFT,
        MINUS;

        public <R> @Nullable R finalRow(boolean emptyRightSide, Merger<R> merger, R leftRow) {
            switch (this) {
                case INNER:
                    return null;
                case LEFT:
                    return emptyRightSide ? merger.merge(leftRow, null) : null;
                case MINUS:
                    return emptyRightSide ? leftRow : null;
            }
            String msg = "No finalRow() implementation for " + this + " fix me";
            throw new UnsupportedOperationException(msg);
        }

        public String operatorName() {
            switch (this) {
                case INNER:
                    return "BindJoin";
                case LEFT:
                    return "LeftBindJoin";
                case MINUS:
                    return "Minus";
            }
            throw new UnsupportedOperationException("No operatorName for "+this+". Fix me");
        }
    }

    private static final Logger log = LoggerFactory.getLogger(BindJoinPublisher.class);
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final int bindConcurrency;
    private final Publisher<R> leftPublisher;
    private final Merger<R> merger;
    private final JoinType joinType;
    private boolean terminated;
    private int leftRequested;
    private boolean requesterThread;
    private boolean leftTerminated;
    private @MonotonicNonNull Subscription leftSubscription;

    public BindJoinPublisher(int bindConcurrency, Publisher<R> leftPublisher, Merger<R> merger,
                             JoinType joinType) {
        super(joinType.operatorName()+"-"+nextId.getAndIncrement(), false);
        setTargetParallelism(bindConcurrency);
        this.joinType = joinType;
        this.bindConcurrency = bindConcurrency;
        this.leftPublisher = leftPublisher;
        this.merger = merger;
    }

    @Override protected synchronized void onTerminate(Throwable cause, boolean cancelled) {
        if (!terminated) {
            terminated = true;
            if (leftSubscription != null)
                leftSubscription.cancel();
        }
    }

    private void runRequestLoop() {
        while (true) {
            int n = 0;
            synchronized (this) {
                if (!leftTerminated)
                    n = bindConcurrency - Math.min(bindConcurrency, leftRequested);
                leftRequested += n;
                if (n == 0) {
                    requesterThread = false;
                    return;
                }
            }
            if (n > 0) {
                log.trace(this+".leftSubscription.request({})", n);
                leftSubscription.request(n);
            }
        }
    }

    public void requestLeft(int completed) {
        if (bindConcurrency > 1) {
            synchronized (this) {
                assert completed <= leftRequested;
                if (!terminated && !leftTerminated) {
                    leftRequested = Math.max(0, leftRequested - completed);
                    if (!requesterThread) {
                        requesterThread = true;
                        Async.async(this::runRequestLoop);
                    }
                }
            }
        } else {
            int n;
            synchronized (this) {
                assert completed <= leftRequested;
                if (terminated || leftTerminated)
                    n = 0;
                else
                    leftRequested = n = bindConcurrency - (leftRequested - completed);

            }
            if (n > 0)
                leftSubscription.request(n);
        }
    }

    @Override public void subscribe(Subscriber<? super R> s) {
        super.subscribe(s);
        if (leftSubscription != null)
            return; // super has already notified error
        leftPublisher.subscribe(new Subscriber<R>() {
            @Override public void onSubscribe(Subscription s) {
                leftSubscription = s;
                if (terminated)
                    s.cancel();
                else
                    requestLeft(0);
            }
            @Override public void onError(Throwable t) {
                log.trace(BindJoinPublisher.this+".leftSubscriber.onError({})", t, t);
                onTerminate(t, false); // do not make new requests
                addPublisher(new EmptyPublisher<>(t));
            }
            @Override public void onComplete() {
                synchronized (BindJoinPublisher.this) {
                    log.trace(BindJoinPublisher.this+".leftSubscriber.onComplete()");
                    leftTerminated = true;
                }
                markCompletable();
            }
            @Override public void onNext(R leftRow) {
                log.trace(BindJoinPublisher.this+".leftSubscriber.onNext({})", leftRow);
                addPublisher(new RightProcessor(leftRow));
            }
        });
    }

    private final class RightProcessor extends AbstractProcessor<R, R> {
        private final R leftRow;
        private volatile boolean empty = true;
        private long requested;
        private @Nullable R finalRow;

        public RightProcessor(R leftRow) {
            super(merger.bind(leftRow).execute().publisher());
            this.leftRow = leftRow;
        }

        @Override protected Subscription createDownstreamSubscription() {
            return new Subscription() {
                @Override public void request(long n) {
                    if (n < 0)
                        completeDownstream(new IllegalArgumentException("negative request of "+n));
                    if (n == 0) return;
                    R emitRow = null;
                    synchronized (RightProcessor.this) {
                        if (terminated.get()) return;
                        requested += n;
                        if (finalRow != null) {
                            emitRow = finalRow;
                            finalRow = null;
                        }
                    }
                    if (emitRow != null) {
                        emit(emitRow);
                        completeDownstream(null);
                    } else {
                        upstream.request(n);
                    }
                }

                @Override public void cancel() {
                    log.trace("{}.Subscription.cancel() {}term", RightProcessor.this,
                            terminated.get() ? "" : "!");
                    if (terminated.compareAndSet(false, true))
                        cancelUpstream();
                }
            };
        }

        @Override protected void handleOnNext(R rightRow) {
            log.trace("{}.handleOnNext({}) {}term requested={} empty={}", this, rightRow,
                    terminated.get() ? "" : "!", requested, empty);
            synchronized (this) {
                assert finalRow == null;
                assert requested > 0;
                requested = Math.max(0, requested-1);
                empty = false;
            }
            if (joinType == JoinType.MINUS) {
                cancelUpstream();
                completeDownstream(null);
            } else {
                emit(merger.merge(leftRow, rightRow));
            }
        }

        @Override public void onComplete() {
            log.trace("{}.onComplete()", this);
            R emitRow;
            boolean complete = true;
            synchronized (this) {
                emitRow = joinType.finalRow(empty, merger, leftRow);
                if (emitRow != null) { // inject a row
                    if (requested > 0) { //deliver final row now
                        requested--;
                    } else { // wait for next request to deliver finalRow
                        finalRow = emitRow;
                        emitRow = null;
                        complete = false;
                    }
                } //else: no final row generated, just complete
            }
            if (emitRow != null)
                emit(emitRow);
            if (complete)
                completeDownstream(null);
        }

        @Override protected void completeDownstream(@Nullable Throwable cause) {
            requestLeft(1);
            super.completeDownstream(cause);
        }

        @Override public String toString() {
            BindJoinPublisher<R> outer = BindJoinPublisher.this;
            if (leftRow instanceof Object[])
                return outer+".RightProcessor{"+Arrays.toString((Object[]) leftRow)+"}";
            else
                return outer+".RightProcessor{"+leftRow+"}";
        }
    }

}
