package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public abstract sealed class BindQuery<B extends Batch<B>>
        extends BindListener
        permits ItBindQuery, EmitBindQuery {
    public final SparqlQuery query;
    private @MonotonicNonNull Plan parsedQuery;
    public final BindType type;
    public final Metrics.@Nullable JoinMetrics metrics;
    public final SparqlQuery nonAskQuery;

    public BindQuery(SparqlQuery query, BindType type, Metrics.@Nullable JoinMetrics metrics) {
        this.metrics = metrics;
        if (type == null)
            throw new IllegalArgumentException("type cannot be null");
        this.nonAskQuery = query;
        this.query = switch (type) {
            case JOIN,LEFT_JOIN          -> query;
            case EXISTS,NOT_EXISTS,MINUS -> query.toAsk();
        };
        this.type = type;
    }

    protected abstract Object bindingsObj();

    public abstract BatchType<B> batchType();
    public abstract Vars bindingsVars();

    public Vars resultVars() { return type.resultVars(bindingsVars(), query.publicVars()); }

    public final Plan parsedQuery() {
        if (parsedQuery == null)
            parsedQuery = SparqlParser.parse(query);
        return parsedQuery;
    }

    @Override public final String toString() {
        return "BindQuery["+ type +"]{query="+query+", bindings="+bindingsObj()+"}";
    }

    @Override public final int hashCode() {
        int h = 31*type.hashCode() + query.hashCode();
        h = 31*h + bindingsObj().hashCode();
        return 31*h + (metrics == null ? 0 : metrics.hashCode());
    }

    @Override public final boolean equals(Object obj) {
        return obj instanceof BindQuery<?> b
                && b.query.equals(query)
                && b.type == type
                && b.bindingsObj().equals(bindingsObj())
                && Objects.equals(b.metrics, metrics);
    }
}
