package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowHashWindowSet;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.model.row.RowOperationsRegistry;
import com.github.alexishuf.fastersparql.client.model.row.RowSet;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.MergePublisher;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.FasterSparqlOpProperties;
import com.github.alexishuf.fastersparql.operators.Merge;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.MergePlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.providers.MergeProvider;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.index.qual.NonNegative;
import org.reactivestreams.Subscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.hasGlobalMetricsListeners;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.sendMetrics;
import static java.lang.System.nanoTime;

@Value @Accessors(fluent = true)
public class SimpleMerge implements Merge {
    RowOperations rowOps;
    boolean parallel;
    int window;

    public static class Provider implements MergeProvider {
        @Override public @NonNegative int bid(long flags) { return BidCosts.BUILTIN_COST; }

        @Override public Merge create(long flags, RowOperations rowOps) {
            boolean parallel = (flags & OperatorFlags.ASYNC) != 0;
            return new SimpleMerge(rowOps, parallel, -1);
        }
    }

    @Override public <R> Class<R> rowClass() {
        //noinspection unchecked
        return (Class<R>) rowOps.rowClass();
    }

    @Override public <R> Results<R> checkedRun(MergePlan<R> plan) {
        List<? extends Plan<R>> operands = plan.operands();
        List<String> vars = plan.publicVars();
        int window = this.window < 0 ? FasterSparqlOpProperties.mergeWindow() : this.window;
        int concurrency = parallel ? operands.size() : 1;
        MergeOperatorPublisher<R> pub = new MergeOperatorPublisher<>(concurrency, window, plan, operands, vars);
        return new Results<>(vars, plan.rowClass(), pub);
    }

    private static final class MergeOperatorPublisher<R> extends MergePublisher<R> {
        private static final AtomicInteger nextId = new AtomicInteger(0);
        private final MergePlan<R> plan;
        private long start = Long.MAX_VALUE, rows = 0;
        private final List<RowSet<R>> windows;

        private class Processor extends AbstractProcessor<R, R> {
            private final Merger<R> merger;
            private final int idx;

            public Processor(int idx,RowOperations rowOps, List<String> outVars, Results<R> source) {
                super(source.publisher());
                this.idx = idx;
                merger = Merger.forProjection(rowOps, outVars, source.vars());
            }

            @Override protected void handleOnNext(R row) {
                R projected = merger.merge(row, null);
                boolean duplicate = false;
                for (int i = 0; !duplicate && i < windows.size(); i++)
                    duplicate = i != idx && windows.get(i).contains(projected);
                if (duplicate) {
                    upstream.request(1);
                } else {
                    windows.get(idx).add(row);
                    emit(projected);
                }
            }
        }

        public MergeOperatorPublisher(int concurrency, int windowSize, MergePlan<R> plan,
                                      List<? extends Plan<R>> operands,
                                      List<String> outVars) {
            super(plan.name()+"["+nextId.getAndIncrement()+"]", concurrency, concurrency,
                    false, null);
            RowOperations rowOps = RowOperationsRegistry.get().forClass(plan.rowClass());
            this.plan = plan;
            this.windows = new ArrayList<>(operands.size());
            int i = 0;
            for (Plan<R> inputPlan : operands) {
                Results<R> results = inputPlan.execute();
                addPublisher(new Processor(i++, rowOps, outVars, results));
                windows.add(new RowHashWindowSet<>(windowSize, rowOps));
            }
            markCompletable();
        }

        @Override public void subscribe(Subscriber<? super R> s) {
            start = nanoTime();
            super.subscribe(s);
        }

        @Override protected void feed(R item) {
            ++rows;
            super.feed(item);
        }

        @Override protected void onComplete(Throwable cause, boolean cancelled) {
            if (hasGlobalMetricsListeners())
                sendMetrics(plan, new PlanMetrics(plan.name(), rows, start, cause, cancelled));
        }
    }
}
