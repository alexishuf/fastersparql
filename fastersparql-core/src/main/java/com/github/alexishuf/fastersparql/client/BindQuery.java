package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public class BindQuery<B extends Batch<B>> {
    public final SparqlQuery query;
    private @MonotonicNonNull Plan parsedQuery;
    public final BIt<B> bindings;
    public final BindType type;
    public Metrics.@Nullable JoinMetrics metrics;

    public BindQuery(SparqlQuery query, BIt<B> bindings, BindType type) {
        if (bindings == null || type == null)
            throw new IllegalArgumentException("bindings and bindType cannot be null");
        this.query = query;
        this.bindings = bindings;
        this.type = type;
    }

    public BindQuery(SparqlQuery query, BIt<B> bindings, BindType type,
                     Metrics.@Nullable JoinMetrics metrics) {
        this(query, bindings, type);
        this.metrics = metrics;
    }

    public Vars resultVars() { return type.resultVars(bindings.vars(), query.publicVars()); }

    public Plan parsedQuery() {
        if (parsedQuery == null)
            parsedQuery = new SparqlParser().parse(query);
        return parsedQuery;
    }

    public void    emptyBinding(long sequence) { }
    public void nonEmptyBinding(long sequence) { }

    @Override public String toString() {
        return "BindQuery["+ type +"]{query="+query+", bindings="+bindings+"}";
    }

    @Override public boolean equals(Object obj) {
        return obj == this || obj instanceof BindQuery<?> q
                && q.bindings == bindings
                && q.type == type
                && Objects.equals(q.metrics, metrics)
                && q.query.equals(query);
    }

    @Override public int hashCode() {
        return Objects.hash(bindings, type, metrics, query);
    }
}
