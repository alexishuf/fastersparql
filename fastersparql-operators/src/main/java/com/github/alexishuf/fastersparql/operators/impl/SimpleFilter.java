package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
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
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.val;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


    public static class Provider implements FilterProvider {
        private static final Logger log = LoggerFactory.getLogger(Provider.class);
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

    @Override public <R> Class<R> rowClass() {
        //noinspection unchecked
        return (Class<R>) rowOperations.rowClass();
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
        val pub = new FilterPublisher<>(left.publisher(), evaluators, plan);
        return new Results<>(left.vars(), rowClass, pub);
    }

    private static class FilterPublisher<R> extends AbstractProcessor<R, R> {
        private final FilterPlan<R> plan;
        private final List<ExprEvaluator<R>> predicates;

        public FilterPublisher(FSPublisher<? extends R> upstream, List<ExprEvaluator<R>> predicates,
                               FilterPlan<R> plan) {
            super(upstream);
            this.predicates = predicates;
            this.plan = plan;
        }

        @Override protected void handleOnNext(R row) {
            boolean discard = false;
            for (ExprEvaluator<R> predicate : predicates) {
                if (!RDFValues.coerceToBool(predicate.evaluate(row))) {
                    discard = true;
                    break;
                }
            }
            if (discard)
                upstream.request(1);
            else
                emit(row);
        }

        @Override protected void onTerminate(@Nullable Throwable error, boolean cancelled) {
            if (hasGlobalMetricsListeners())
                sendMetrics(plan, new PlanMetrics(plan.name(), rows, start, error, cancelled));
        }
    }
}
