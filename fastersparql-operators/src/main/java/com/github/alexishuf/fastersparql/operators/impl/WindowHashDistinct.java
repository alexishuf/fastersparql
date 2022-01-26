package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.Distinct;
import com.github.alexishuf.fastersparql.operators.FasterSparqlOpProperties;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.providers.DistinctProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import org.checkerframework.checker.index.qual.NonNegative;
import org.reactivestreams.Publisher;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.function.Function;

import static com.github.alexishuf.fastersparql.operators.OperatorFlags.*;

public class WindowHashDistinct implements Distinct {
    private final Function<Object, Object> wrap;
    private final int overrideWindow;

    public static class Provider implements DistinctProvider {
        @Override public @NonNegative int bid(long flags) {
            if ((flags & ALLOW_DUPLICATES) == 0)
                return BidCosts.UNSUPPORTED;
            return BidCosts.BUILTIN_COST + 8 * BidCosts.SLOW_COST
                    + (((flags &     ASYNC) != 0) ? BidCosts.MINOR_COST : 0)
                    + (((flags & SPILLOVER) != 0) ? BidCosts.MINOR_COST : 0);
        }

        @Override public Distinct create(long flags, RowOperations rowOperations) {
            return new WindowHashDistinct(rowOperations);
        }
    }

    public WindowHashDistinct(RowOperations rowOperations) {
        this(rowOperations, -1);
    }

    public WindowHashDistinct(RowOperations rowOperations, int overrideWindow) {
        this.wrap = !rowOperations.needsCustomHash() ? Function.identity()
                  : r -> new InMemoryHashDistinct.HashAdapter(rowOperations, r);
        this.overrideWindow = overrideWindow;
    }

    @Override public <R> Results<R> checkedRun(Plan<R> inputPlan) {
        Results<R> in = inputPlan.execute();
        int window = overrideWindow >= 0 ? overrideWindow
                                         : FasterSparqlOpProperties.distinctWindow();
        return new Results<>(in.vars(), in.rowClass(), new Hasher<>(in.publisher(), wrap, window));
    }

    private static final class Hasher<R> extends AbstractProcessor<R, R> {
        private final Function<Object, Object> wrap;
        private final int windowSize;
        private final LinkedHashSet<Object> window;

        public Hasher(Publisher<? extends R> source, Function<Object, Object> wrap,
                      int windowSize) {
            super(source);
            this.wrap = wrap;
            this.windowSize = windowSize;
            this.window = new LinkedHashSet<>(windowSize); //underestimates actual capacity
        }

        @Override protected void handleOnNext(R item) {
            if (window.add(wrap.apply(item))) {
                if (window.size() > windowSize)
                    removeOldest();
                emit(item);
            }
        }

        private void removeOldest() {
            Iterator<Object> it = window.iterator();
            if (it.hasNext()) {
                it.next();
                it.remove();
            }
        }
    }
}
