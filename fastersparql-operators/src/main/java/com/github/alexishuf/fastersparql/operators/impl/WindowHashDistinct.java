package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.Distinct;
import com.github.alexishuf.fastersparql.operators.FasterSparqlOpProperties;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.DistinctPlan;
import com.github.alexishuf.fastersparql.operators.providers.DistinctProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.function.Function;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.hasGlobalMetricsListeners;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.sendMetrics;
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

    @Override public <R> Results<R> checkedRun(DistinctPlan<R> plan) {
        Results<R> in = plan.input().execute();
        int window = overrideWindow >= 0 ? overrideWindow
                                         : FasterSparqlOpProperties.distinctWindow();
        return new Results<>(in.vars(), in.rowClass(), new Hasher<>(in, wrap, window, plan));
    }

    private static final class Hasher<R> extends AbstractProcessor<R, R> {
        private final DistinctPlan<R> plan;
        private final Class<? super R> rowClass;
        private final Function<Object, Object> wrap;
        private final int windowSize;
        private final LinkedHashSet<Object> window;

        public Hasher(Results<R> results, Function<Object, Object> wrap,
                      int windowSize, DistinctPlan<R> plan) {
            super(results.publisher());
            this.wrap = wrap;
            this.windowSize = windowSize;
            this.window = new LinkedHashSet<>(windowSize); //underestimates actual capacity
            this.plan = plan;
            this.rowClass = results.rowClass();
        }

        @Override protected void handleOnNext(R item) {
            if (window.add(wrap.apply(item))) {
                if (window.size() > windowSize)
                    removeOldest();
                emit(item);
            }
        }

        @Override protected void onTerminate(@Nullable Throwable error, boolean cancelled) {
            if (hasGlobalMetricsListeners()) {
                sendMetrics(new PlanMetrics<>(plan, rowClass, rows,
                                              start, System.nanoTime(), error, cancelled));
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
