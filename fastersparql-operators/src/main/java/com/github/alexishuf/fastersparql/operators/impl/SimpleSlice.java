package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.Slice;
import com.github.alexishuf.fastersparql.operators.providers.SliceProvider;
import org.reactivestreams.Publisher;

public class SimpleSlice implements Slice {

    /** Creates {@link Slice} instances. */
    public static class Provider implements SliceProvider {

        @Override public int bid(long flags) {
            int bid = BidCosts.BUILTIN_COST;
            if ((flags & OperatorFlags.ASYNC) > 0) bid += BidCosts.MINOR_COST;
            return bid;
        }

        @Override public Slice create(long flags) {
            return new SimpleSlice();
        }
    }

    @Override public <R> Results<R> run(Results<R> input, long offset, long limit) {
        return new Results<>(input.vars(), input.rowClass(),
                             new SlicingProcessor<>(input.publisher(), offset, limit));
    }

    private static class SlicingProcessor<R> extends AbstractProcessor<R> {
        private final long offset, limit;
        private long itemsReceived = 0;

        public SlicingProcessor(Publisher<? extends R> source, long offset, long limit) {
            super(source);
            this.offset = offset;
            this.limit = limit;
        }

        @Override public void onNext(R r) {
            if (itemsReceived++ >= offset) {
                if (itemsReceived-offset <= limit) {
                    emit(r);
                } else if (!terminated) {
                    cancelUpstream();
                    completeDownstream(null);
                }

            }
        }
    }
}
