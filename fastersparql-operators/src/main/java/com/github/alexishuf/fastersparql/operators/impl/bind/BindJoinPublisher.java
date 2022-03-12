package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.EmptyPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.MergePublisher;
import com.github.alexishuf.fastersparql.operators.impl.Merger;
import com.github.alexishuf.fastersparql.operators.metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import lombok.val;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.hasGlobalMetricsListeners;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.sendMetrics;

class BindJoinPublisher<R> extends MergePublisher<R> {
    public enum JoinType {
        INNER,
        LEFT,
        MINUS,
        FILTER_NOT_EXISTS,
        FILTER_EXISTS;

        public <R> @Nullable R finalRow(boolean emptyRightSide, Merger<R> merger, R leftRow) {
            switch (this) {
                case INNER:
                    return null;
                case LEFT:
                    return emptyRightSide ? merger.merge(leftRow, null) : null;
                case MINUS:
                case FILTER_NOT_EXISTS:
                    return emptyRightSide ? leftRow : null;
                case FILTER_EXISTS:
                    return emptyRightSide ? null : leftRow;
            }
            String msg = "No finalRow() implementation for " + this + " fix me";
            throw new UnsupportedOperationException(msg);
        }

        public boolean isExistsFilter() {
            switch (this) {
                case MINUS:
                case FILTER_NOT_EXISTS:
                case FILTER_EXISTS:
                    return true;
            }
            return false;
        }

        public String operatorName() {
            switch (this) {
                case INNER:
                    return "BindJoin";
                case LEFT:
                    return "LeftBindJoin";
                case MINUS:
                    return "Minus";
                case FILTER_NOT_EXISTS:
                    return "FilterNotExists";
                case FILTER_EXISTS:
                    return "FilterExists";
            }
            throw new UnsupportedOperationException("No operatorName for "+this+". Fix me");
        }
    }

    /* --- --- --- constants and class-level state --- --- --- */

    private static final Logger log = LoggerFactory.getLogger(BindJoinPublisher.class);
    private static final AtomicInteger nextId = new AtomicInteger(1);

    /* --- --- --- immutable state state --- --- --- */

    private final @Positive int bindConcurrency;
    private final FSPublisher<R> leftPublisher;
    private final Merger<R> merger;
    private final JoinType joinType;
    private final Plan<R> originalPlan;

    /* --- --- --- left-side state --- --- --- */

    private @NonNegative int leftRequested;
    private boolean leftActive, loggedLeftNotActive;
    private @MonotonicNonNull Subscription leftSubscription;

    /* --- --- --- metrics --- --- --- */
    private long start = Long.MAX_VALUE, rows;
    private long leftRows, leftUnmatched, totalRightRows, maxRightRows;

    /* --- --- --- constructors --- --- --- */

    public BindJoinPublisher(int bindConcurrency, Publisher<R> leftPublisher, Merger<R> merger,
                             JoinType joinType, Plan<R> originalPlan) {
        super(joinType.operatorName()+"-"+nextId.getAndIncrement(),
                bindConcurrency, bindConcurrency, false, null);
        this.joinType = joinType;
        this.bindConcurrency = bindConcurrency;
        this.leftPublisher = FSPublisher.bind(leftPublisher, executor());
        this.merger = merger;
        this.originalPlan = originalPlan;
    }

    /* --- --- --- public interface --- --- --- */

    @Override public void subscribe(Subscriber<? super R> downstream) {
        start = System.nanoTime();
        if (leftSubscription == null) {
            leftPublisher.moveTo(executor());
            leftPublisher.subscribe(leftSubscriber);
        }
        //else: super will deliver error to downstream
        super.subscribe(downstream);
    }

    /* --- --- --- hooks --- --- --- */

    @Override protected void onRequest(long n) {
        super.onRequest(n);
        requestLeft(0);
    }

    @Override protected void onComplete(Throwable cause, boolean cancelled) {
        super.onComplete(cause, cancelled);
        if (leftActive && (cancelled || cause != null)) {
            leftActive = false;
            leftSubscription.cancel();
        }
        if (hasGlobalMetricsListeners()) {
            double leftUnmatchedRate = leftUnmatched / (double) rows;
            double avgRightMatches = totalRightRows / (double) rows;
            val metrics = new JoinMetrics(originalPlan.name(), rows,
                                          start, System.nanoTime(), cause, cancelled, leftRows,
                                          leftUnmatchedRate, avgRightMatches, maxRightRows);
            sendMetrics(originalPlan, metrics);
        }
    }

    @Override protected void feed(R item) {
        super.feed(item);
        ++rows;
    }

    /* --- --- --- left subscription management --- --- --- */

    private final Subscriber<R> leftSubscriber = new Subscriber<R>() {
        @Override public void onSubscribe(Subscription s) {
            assert leftSubscription == null : "already has leftSubscription!";
            leftSubscription = s;
            leftActive = true;
        }
        @Override public void onError(Throwable error) {
            log.trace("{}.onError({})", this, Objects.toString(error));
            addPublisher(new EmptyPublisher<>(error));
        }
        @Override public void onComplete() {
            log.trace("{}.onComplete()", this);
            leftActive = false;
            markCompletable();
        }
        @Override public void onNext(R leftRow) {
            ++leftRows;
            addPublisher(new RightProcessor(leftRow));
        }
        @Override public String toString() { return BindJoinPublisher.this+"leftSubscriber"; }
    };

    private void requestLeft(@NonNegative int completed) {
        if (!assertEventThread()) {
            executor().execute(() -> requestLeft(completed));
            return;
        }
        assert leftSubscription != null : "requestLeft called before subscribe";
        assert completed <= leftRequested : "more completions than requested";
        assert leftRequested <= bindConcurrency : "leftRequested above allowed concurrency";
        if (leftActive) {
            int n = bindConcurrency - (leftRequested -= completed);
            if (n > 0) {
                leftRequested += n;
                leftSubscription.request(n);
            }
        } else if (!loggedLeftNotActive) {
            log.debug("{}.requestLeft({}): left subscription not active anymore", this, completed);
            loggedLeftNotActive = true;
        }
    }

    /* --- --- --- right subscription management --- --- --- */

    private FSPublisher<R> createRightUpstream(R leftRow) {
        return FSPublisher.bind(merger.bind(leftRow).execute().publisher(), executor());
    }

    private final class RightProcessor extends AbstractProcessor<R, R> {
        private final R leftRow;
        private volatile boolean empty = true;
        private long requested;
        private @Nullable R finalRow;
        private boolean logicalCompleted;

        public RightProcessor(R leftRow) {
            super(createRightUpstream(leftRow));
            this.leftRow = leftRow;
        }

        @Override protected Subscription createDownstreamSubscription() {
            return new Subscription() {
                @Override public void request(long n) {
                    if (n < 0)
                        completeDownstream(new IllegalArgumentException("negative request of "+n));
                    if (n == 0) return;
                    R emitRow = null;
                    if (terminated.get())
                        return;
                    requested += n;
                    if (finalRow != null) {
                        emitRow = finalRow;
                        finalRow = null;
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
                    if (terminated.compareAndSet(false, true)) {
                        cancelUpstream();
                        onTerminate(null, true);
                    }
                }
            };
        }

        @Override protected void handleOnNext(R rightRow) {
            log.trace("{}.handleOnNext({}) {}term requested={} empty={}", this, rightRow,
                      terminated.get() ? "" : "!", requested, empty);
            assert finalRow == null;
            assert requested > 0;
            requested = Math.max(0, requested-1);
            empty = false;
            if (joinType.isExistsFilter()) {
                cancelUpstream();
                logicalComplete();
            } else {
                emit(merger.merge(leftRow, rightRow));
            }
        }

        private void logicalComplete() {
            if (logicalCompleted) {
                log.trace("{}.logicalComplete(): no-op", this);
                return;
            }
            logicalCompleted = true;
            saveMetrics();
            R emitRow;
            boolean complete = true;
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
            if (emitRow != null)
                emit(emitRow);
            if (complete)
                completeDownstream(null);
        }

        private void saveMetrics() {
            if (empty)
                ++leftUnmatched;
            maxRightRows = Math.max(maxRightRows, rows);
            totalRightRows += rows;
        }

        @Override public void onComplete() {
            log.trace("{}.onComplete()", this);
            logicalComplete();
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
