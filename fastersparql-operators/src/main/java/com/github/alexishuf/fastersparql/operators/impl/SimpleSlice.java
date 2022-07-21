package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.Slice;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.SlicePlan;
import com.github.alexishuf.fastersparql.operators.providers.SliceProvider;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.val;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.hasGlobalMetricsListeners;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.sendMetrics;

@Value @Accessors(fluent = true)
public class SimpleSlice implements Slice {
    private static final Logger log = LoggerFactory.getLogger(SimpleSlice.class);
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
            val processor = new SliceProcessor<>(input.publisher(), plan.offset(), plan.limit(),
                                                   plan);
            return new Results<>(input.vars(), input.rowClass(), processor);
        } catch (Throwable t) {
            return Results.error(Object.class, t);
        }
    }

    private static class SliceProcessor<R> extends AbstractProcessor<R, R> {
        private final SlicePlan<R> plan;
        private final @NonNegative long offset, limit;
        private long itemsReceived = 0;

        public SliceProcessor(FSPublisher<? extends R> source, long offset, long limit,
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
                    log.debug("{}: limit reached!", this);
                    cancelUpstream();
                    completeDownstream(null);
                }
            }
        }

        @Override protected void onTerminate(@Nullable Throwable error, boolean cancelled) {
            if (hasGlobalMetricsListeners())
                sendMetrics(plan, new PlanMetrics(plan.name(), rows, start, error, cancelled));
        }

        @Override public String toString() {
            String status = terminated.get() ? "terminated"
                                             : (downstream == null ? "not subscribed" : "active");
            return String.format("SliceProcessor[%s:+%d](%s)-%s",
                                 plan.offset(), plan.limit(),  status,  source.toString());
        }
    }
}
