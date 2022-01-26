package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.Slice;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.providers.SliceProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import org.checkerframework.checker.index.qual.NonNegative;
import org.reactivestreams.Publisher;

public class SimpleSlice implements Slice {
    public static class Provider implements SliceProvider {
        @Override public int bid(long flags) {
            int bid = BidCosts.BUILTIN_COST;
            if ((flags & OperatorFlags.ASYNC) != 0) bid += BidCosts.MINOR_COST;
            return bid;
        }
        @Override public Slice create(long flags, RowOperations ops) {
            return new SimpleSlice();
        }
    }

    @Override public <R> Results<R> run(Plan<R> inputPlan,
                                        @NonNegative long offset, @NonNegative long limit) {
        try {
            Results<R> input = inputPlan.execute();
            return new Results<>(input.vars(), input.rowClass(),
                                 new SlicingProcessor<>(input.publisher(), offset, limit));
        } catch (Throwable t) {
            return Results.forError(Object.class, t);
        }
    }

    private static class SlicingProcessor<R> extends AbstractProcessor<R, R> {
        private final @NonNegative long offset, limit;
        private long itemsReceived = 0;

        public SlicingProcessor(Publisher<? extends R> source, long offset, long limit) {
            super(source);
            this.offset = offset;
            this.limit = limit;
        }

        @Override protected void handleOnNext(R item) {
            if (itemsReceived++ >= offset) {
                if (itemsReceived-offset <= limit) {
                    emit(item);
                } else if (!terminated) {
                    cancelUpstream();
                    completeDownstream(null);
                }
            }
        }
    }
}
