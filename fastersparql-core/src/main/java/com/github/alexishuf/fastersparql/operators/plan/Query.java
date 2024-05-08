package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public final class Query extends Plan {
    public SparqlQuery sparql;
    public SparqlClient client;

    public Query(SparqlQuery sparql, SparqlClient client) {
        super(Operator.QUERY);
        this.sparql = sparql;
        this.client = client;
    }

    @Override public Plan copy(@Nullable Plan[] ignored) {
        return new Query(sparql, client);
    }

    public SparqlQuery      query() { return sparql; }
    public SparqlClient client() { return client; }

    @Override public <B extends Batch<B>> BIt<B> execute(BatchType<B> batchType, @Nullable Binding binding, boolean weakDedup) {
        var sparql = this.sparql;
        if (binding != null) sparql = sparql.bound(binding);
        if (weakDedup)       sparql = sparql.toDistinct(client.cheapestDistinct());
        return client.query(batchType, sparql).metrics(Metrics.createIf(this));
    }

    @Override
    public <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    doEmit(BatchType<B> type, Vars rebindHint, boolean weakDedup) {
        var sparql = weakDedup ? this.sparql.toDistinct(client.cheapestDistinct()) : this.sparql;
        return client.emit(type, sparql, rebindHint);
    }

    @Override public boolean equals(Object o) {
        return o instanceof Query q && sparql.equals(q.sparql)
                && client.equals(q.client);
    }

    @Override public int hashCode() {
        return Objects.hash(sparql, client);
    }
}
