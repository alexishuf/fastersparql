package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Operator;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.Query;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SingletonFederator extends Optimizer {
    protected final SparqlClient client;
    protected final CardinalityEstimator estimator;
    private final BatchType<?> preferredBatchType;

    public SingletonFederator(SparqlClient client, BatchType<?> preferredBatchType) {
        this.client = client;
        this.preferredBatchType = preferredBatchType;
        this.estimator = this;
        estimator(client, this);
    }
    public SingletonFederator(SparqlClient client, BatchType<?> preferredBatchType,
                              CardinalityEstimator estimator) {
        this.client = client;
        this.preferredBatchType = preferredBatchType;
        this.estimator = estimator;
        estimator(client, estimator);
    }

    public <B extends Batch<B>> BIt<B> execute(BatchType<B> batchType, Plan plan) {
        Vars pubVars = plan.publicVars();
        plan = Federation.copySanitize(plan);
        plan = optimize(plan, Vars.EMPTY);
        plan = bind2client(plan, QueryMode.ITERATOR);
        plan = FS.project(plan, pubVars);
        return batchType.convert(plan.execute(preferredBatchType));
    }

    public <B extends Batch<B>> Emitter<B> emit(BatchType<B> type, Plan plan, Vars rebindHint) {
        Vars pubVars = plan.publicVars();
        plan = Federation.copySanitize(plan);
        plan = optimize(plan, rebindHint);
        plan = bind2client(plan, QueryMode.EMIT);
        plan = FS.project(plan, pubVars);
        var emitter = plan.emit(preferredBatchType, rebindHint);
        //noinspection unchecked
        return preferredBatchType.equals(type) ? (Emitter<B>) emitter : convert(type, emitter);
    }

    protected <B extends Batch<B>> Emitter<B> convert(BatchType<B> dest, Emitter<?> in) {
        return dest.convert(in);
    }

    @Override public int estimate(TriplePattern tp, @Nullable Binding binding) {
       return estimator.estimate(tp, binding);
    }

    @Override public int estimate(Query q, @Nullable Binding binding) {
        if (q.client != client)
            throw new IllegalArgumentException("Unexpected client");
        Plan parsed = q.sparql instanceof Plan p ? p : SparqlParser.parse(q.sparql);
        return estimator.estimate(parsed);
    }

    protected enum QueryMode {
        ITERATOR,
        EMIT
    }

    protected Plan bind2client(Plan plan, QueryMode mode) {
        return plan.type == Operator.TRIPLE ? new Query(plan, client) : bindInner(plan, mode);
    }

    protected Plan bindInner(Plan plan, QueryMode mode) {
        for (int i = 0, n = plan.opCount(); i < n; i++) {
            Plan o = plan.op(i), bound = bind2client(o, mode);
            if (bound != o) plan.replace(i, bound);
        }
        return plan;
    }
}
