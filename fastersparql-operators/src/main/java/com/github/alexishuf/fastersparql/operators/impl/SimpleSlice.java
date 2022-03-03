package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.Slice;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.SlicePlan;
import com.github.alexishuf.fastersparql.operators.providers.SliceProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.val;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.hasGlobalMetricsListeners;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.sendMetrics;
import static java.lang.System.nanoTime;

@Value @Accessors(fluent = true)
public class SimpleSlice implements Slice {
    Class<?> rowClass;

    public static class Provider implements SliceProvider {
        @Override public int bid(long flags) {
            int bid = BidCosts.BUILTIN_COST;
            if ((flags & OperatorFlags.ASYNC) != 0) bid += BidCosts.MINOR_COST;
            return bid;
        }
        @Override public Slice create(long flags, RowOperations ops) {
            return new SimpleSlice(ops.rowClass());
        }
    }



    @Override public <R> Results<R> run(SlicePlan<R> plan) {
        try {
            Results<R> input = plan.input().execute();
            val processor = new SlicingProcessor<>(input.publisher(), plan.offset(), plan.limit(),
                                                   plan);
            return new Results<>(input.vars(), input.rowClass(), processor);
        } catch (Throwable t) {
            return Results.error(Object.class, t);
        }
    }

    private static class SlicingProcessor<R> extends AbstractProcessor<R, R> {
        private final SlicePlan<R> plan;
        private final @NonNegative long offset, limit;
        private long itemsReceived = 0;

        public SlicingProcessor(Publisher<? extends R> source, long offset, long limit,
                                SlicePlan<R> plan) {
            super(source);
            this.offset = offset;
            this.limit = limit;
            this.plan = plan;
        }

        @Override protected void handleOnNext(R item) {
            if (itemsReceived++ >= offset) {
                if (itemsReceived-offset <= limit) {
                    emit(item);
                } else if (!terminated.get()) {
                    cancelUpstream();
                    completeDownstream(null);
                }
            }
        }

        @Override protected void onTerminate(@Nullable Throwable error, boolean cancelled) {
            if (hasGlobalMetricsListeners()) {
                val m = new PlanMetrics(plan.name(), rows, start, nanoTime(), error, cancelled);
                sendMetrics(plan, m);
            }
        }
    }
}
