package com.github.alexishuf.fastersparql.operators.metrics;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.github.alexishuf.fastersparql.batch.BItClosedAtException.isClosedFor;

@SuppressWarnings("unused")
public class Metrics {
    /** {@link Plan} for which these metrics apply */
    public Plan plan;
    /** Total number of rows emitted until a cancel or completion */
    public long rows;
    /** Total number of non-empty batches of rows emitted until a cancel or completion */
    public long batches;
    /** {@link System#nanoTime()} when execution started */
    public long startNanos;
    /** {@link System#nanoTime()} when execution completed (cancel or completion) */
    public long endNanos;
    /** If execution ended due to an error, this is the {@link Throwable} instance. */
    public @Nullable Throwable error;
    /** Whether execution was cancelled by a downstream consumer (and not due to an error).
     *  Note that {@link Metrics#error} may still be non-null if this is true. */
    public boolean cancelled;

    /** How many nanoseconds until the first batch was emitted */
    public long firstRowNanos;
    /** Minimum delta (in nanos) between two consecutive emission of row batches. */
    public long minNanosBetweenBatches;
    /** Maximum delta (in nanos) between two consecutive emission of row batches. */
    public long maxNanosBetweenBatches;
    /** How many nanos elapsed between the last non-empty Batch and the terminal empty
     *  {@link Batch} was emitted */
    public long terminalNanos;

    /** Average number of rows yielded for every binding of the right operand  */
    public final class JoinMetrics {
        /** How many times this operand was bound to a row of its left-side siblings */
        public int bindings;
        /** On average a binding of this operand yielded this many result rows */
        public double avgRowsPerBinding;
        /** If this operand was bound {@code n} times, {@code rate*n} is the number of times
         *  the binding yielded zero result rows */
        public double noRowsPerBindingRate;
        /** Largest number of result rows yielded for any binding */
        public long maxRowsPerBinding;
        /** Minimal duration (in nanos) from first to last result row for any binding */
        public long minNanosPerBinding;
        /** Maximum duration (in nanos) from first to last result row for any binding */
        public long maxNanosPerBinding;
        /** On average, it took this many nanos to fetch all result rows for a binding */
        public long avgNanosPerBinding;

        private final boolean isLast;

        private double leftRowsUnmatched;
        private long totalNanosPerLeftRow;
        private long leftRowReceived;
        private long totalRightMatches, rightMatches;

        public JoinMetrics(boolean isLast) {
            this.isLast = isLast;
        }

        public void beginBinding() {
            long now = System.nanoTime();
            if (bindings > 0)
                bindingExhausted(now);
            ++bindings;
            rightMatches = 0;
            leftRowReceived = now;
        }

        private void bindingExhausted(long now) {
            if (rightMatches == 0) {
                leftRowsUnmatched++;
            } else {
                totalRightMatches += rightMatches;
                if (rightMatches > maxRowsPerBinding)
                    maxRowsPerBinding = rightMatches;
            }

            long nanos = now - leftRowReceived;
            if (nanos < minNanosPerBinding)
                minNanosPerBinding = nanos;
            if (nanos > maxNanosPerBinding)
                maxNanosPerBinding = nanos;
            totalNanosPerLeftRow += nanos;
        }

        public void rightRowsReceived(int n) {
            rightMatches += n;
            if (isLast)
                rowsEmitted(n);
        }

        public void complete() {
            bindingExhausted(System.nanoTime());
            avgRowsPerBinding = totalRightMatches/(double)bindings;
            noRowsPerBindingRate = leftRowsUnmatched/(double)bindings;
            avgNanosPerBinding = totalNanosPerLeftRow/bindings;
        }

        public void completeAndDeliver(@Nullable Throwable cause, BIt<?> it) {
            complete();
            if (isLast)
                Metrics.this.complete(cause, isClosedFor(cause, it));
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof JoinMetrics that)) return false;
            return bindings == that.bindings && Double.compare(that.avgRowsPerBinding, avgRowsPerBinding) == 0 && Double.compare(that.noRowsPerBindingRate, noRowsPerBindingRate) == 0 && maxRowsPerBinding == that.maxRowsPerBinding && minNanosPerBinding == that.minNanosPerBinding && maxNanosPerBinding == that.maxNanosPerBinding && avgNanosPerBinding == that.avgNanosPerBinding;
        }

        @Override public int hashCode() {
            return Objects.hash(bindings, avgRowsPerBinding, noRowsPerBindingRate, maxRowsPerBinding, minNanosPerBinding, maxNanosPerBinding, avgNanosPerBinding);
        }

        @Override public String toString() {
            return "JoinMetrics{" +
                    "bindings=" + bindings +
                    ", avgRowsPerBinding=" + avgRowsPerBinding +
                    ", noRowsPerBindingRate=" + noRowsPerBindingRate +
                    ", maxRowsPerBinding=" + maxRowsPerBinding +
                    ", minNanosPerBinding=" + minNanosPerBinding +
                    ", maxNanosPerBinding=" + maxNanosPerBinding +
                    ", avgNanosPerBinding=" + avgNanosPerBinding +
                    '}';
        }
    }
    /** Metrics for each join operand. Fir the first operand (index 0), metrics are computed as if
     *  there was a single "dummy" binding of no vars. */
    public final JoinMetrics[] joinMetrics;

    private static final JoinMetrics[] EMPTY_JOIN_METRICS = new JoinMetrics[0];

    private long lastEmit;

    public Metrics(Plan plan) {
        this.plan = plan;
        this.lastEmit = startNanos = System.nanoTime();
        firstRowNanos = -1;
        joinMetrics = switch (plan.type) {
            case JOIN,LEFT_JOIN,EXISTS,NOT_EXISTS,MINUS -> new JoinMetrics[plan.opCount()];
            default -> EMPTY_JOIN_METRICS;
        };
        for (int i = 0, last = joinMetrics.length-1; i < joinMetrics.length; i++)
            joinMetrics[i] = new JoinMetrics(i == last);
    }

    public static @Nullable Metrics createIf(Plan plan) {
        return plan.listeners().isEmpty() ? null : new Metrics(plan);
    }

    public void rowsEmitted(int n) {
        if (n <= 0)
            return;
        long now = System.nanoTime(), delta = now - lastEmit;
        if (delta < minNanosBetweenBatches)
            minNanosBetweenBatches = delta;
        if (delta > maxNanosBetweenBatches)
            maxNanosBetweenBatches = delta;
        if (firstRowNanos == -1)
            firstRowNanos = delta;
        lastEmit = now;
        rows += n;
        ++batches;
    }

    public Metrics complete(@Nullable Throwable t, boolean cancelled) {
        terminalNanos = System.nanoTime()-lastEmit;
        error = t;
        this.cancelled = cancelled;
        if (joinMetrics.length > 0) {
            joinMetrics[0].bindings = 1;
        }
        return this;
    }

    public void deliver() {
        for (var listener : plan.listeners())
            listener.accept(this);
    }

    public long time(TimeUnit unit) {
        return unit.convert(endNanos - startNanos, TimeUnit.NANOSECONDS);
    }

    public long avgBatchDelay(TimeUnit unit) {
        return unit.convert((endNanos-startNanos)/batches, TimeUnit.NANOSECONDS);
    }
    public long avgRowDelay(TimeUnit unit) {
        return unit.convert((endNanos-startNanos)/rows, TimeUnit.NANOSECONDS);
    }


    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Metrics metrics)) return false;
        return rows == metrics.rows && batches == metrics.batches && startNanos == metrics.startNanos && endNanos == metrics.endNanos && cancelled == metrics.cancelled && firstRowNanos == metrics.firstRowNanos && minNanosBetweenBatches == metrics.minNanosBetweenBatches && maxNanosBetweenBatches == metrics.maxNanosBetweenBatches && terminalNanos == metrics.terminalNanos && plan.equals(metrics.plan) && Objects.equals(error, metrics.error) && Arrays.equals(joinMetrics, metrics.joinMetrics);
    }

    @Override public int hashCode() {
        int result = Objects.hash(plan, rows, batches, startNanos, endNanos, error, cancelled, firstRowNanos, minNanosBetweenBatches, maxNanosBetweenBatches, terminalNanos);
        result = 31 * result + Arrays.hashCode(joinMetrics);
        return result;
    }

    @Override public String toString() {
        return "Metrics{" +
                ", rows=" + rows +
                ", batches=" + batches +
                ", startNanos=" + startNanos +
                ", endNanos=" + endNanos +
                ", error=" + error +
                ", cancelled=" + cancelled +
                ", firstRowNanos=" + firstRowNanos +
                ", minNanosBetweenBatches=" + minNanosBetweenBatches +
                ", maxNanosBetweenBatches=" + maxNanosBetweenBatches +
                ", terminalNanos=" + terminalNanos +
                ", joinMetrics=" + Arrays.toString(joinMetrics) +
                ", plan=" + plan +
                '}';
    }
}
