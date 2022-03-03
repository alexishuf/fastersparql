package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.FasterSparqlOpProperties;
import com.github.alexishuf.fastersparql.operators.Filter;
import com.github.alexishuf.fastersparql.operators.expressions.ExprEvaluator;
import com.github.alexishuf.fastersparql.operators.expressions.ExprEvaluatorCompiler;
import com.github.alexishuf.fastersparql.operators.expressions.ExprEvaluatorCompilerRegistry;
import com.github.alexishuf.fastersparql.operators.expressions.RDFValues;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.FilterPlan;
import com.github.alexishuf.fastersparql.operators.providers.FilterProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.hasGlobalMetricsListeners;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.sendMetrics;

@Value @Accessors(fluent = true)
public class SimpleFilter implements Filter {
    RowOperations rowOperations;
    ExprEvaluatorCompiler compiler;

    @Slf4j
    public static class Provider implements FilterProvider {
        private @MonotonicNonNull List<ExprEvaluatorCompiler> compilerCache = null;

        private ExprEvaluatorCompiler findCompiler() {
            if (compilerCache == null) {
                String name = FasterSparqlOpProperties.preferredExprCompiler();
                ExprEvaluatorCompiler compiler = ExprEvaluatorCompilerRegistry.get().preferred(name);
                compilerCache = Collections.singletonList(compiler);
            }
            return compilerCache.get(0);
        }

        @Override public @NonNegative int bid(long flags) {
            if (findCompiler() == null) {
                log.warn("No ExprEvaluatorCompiler in ExprEvaluatorCompilerRegistry.get(), " +
                         "will bid UNSUPPORTED");
                return BidCosts.UNSUPPORTED;
            }
            return BidCosts.BUILTIN_COST;
        }

        @Override public Filter create(long flags, RowOperations rowOperations) {
            ExprEvaluatorCompiler compiler = findCompiler();
            if (compiler == null)
                throw new UnsupportedOperationException("No ExprEvaluatorCompiler is available");
            return new SimpleFilter(rowOperations, compiler);
        }
    }

    @Override
    public <R> Results<R> checkedRun(FilterPlan<R> plan) {
        Results<R> left = plan.input().execute();
        Collection<? extends CharSequence> filters = plan.filters();
        if (filters == null || filters.isEmpty())
            return left;
        Class<? super R> rowClass = left.rowClass();
        List<ExprEvaluator<R>> evaluators = new ArrayList<>();
        for (CharSequence expr : filters) {
            evaluators.add(compiler.compile(rowClass, rowOperations, left.vars(), expr));
        }
        val pub = new FilterPublisher<>(left.publisher(), evaluators, plan, left.rowClass());
        return new Results<>(left.vars(), rowClass, pub);
    }

    private static class FilterPublisher<R> extends AbstractProcessor<R, R> {
        private final FilterPlan<R> plan;
        private final Class<? super R> rowClass;
        private final List<ExprEvaluator<R>> predicates;

        public FilterPublisher(Publisher<? extends R> upstream, List<ExprEvaluator<R>> predicates,
                               FilterPlan<R> plan, Class<? super R> rowClass) {
            super(upstream);
            this.predicates = predicates;
            this.plan = plan;
            this.rowClass = rowClass;
        }

        @Override protected void handleOnNext(R row) {
            for (ExprEvaluator<R> predicate : predicates) {
                if (!RDFValues.coerceToBool(predicate.evaluate(row)))
                    return; // discard row
            }
            emit(row);
        }

        @Override protected void onTerminate(@Nullable Throwable error, boolean cancelled) {
            if (hasGlobalMetricsListeners()) {
                sendMetrics(new PlanMetrics<>(plan, rowClass, rows,
                                              start, System.nanoTime(), error, cancelled));
            }
        }
    }
}
