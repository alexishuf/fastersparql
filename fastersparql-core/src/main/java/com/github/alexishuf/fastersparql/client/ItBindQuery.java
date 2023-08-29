package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

public non-sealed class ItBindQuery<B extends Batch<B>> extends BindQuery<B> {
    public final BIt<B> bindings;

    public ItBindQuery(SparqlQuery query, BIt<B> bindings, BindType type) {
        this(query, bindings, type, null);
    }

    public ItBindQuery(SparqlQuery query, BIt<B> bindings, BindType type,
                       Metrics.@Nullable JoinMetrics metrics) {
        super(query, type, metrics);
        if (bindings == null)
            throw new IllegalArgumentException("bindings cannot be null");
        this.bindings = bindings;
    }

    @Override public    Vars         bindingsVars() { return bindings.vars(); }
    @Override public    BatchType<B> batchType()    { return bindings.batchType(); }
    @Override protected Object       bindingsObj()  { return bindings; }
}
