package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

import static com.github.alexishuf.fastersparql.sparql.SparqlQuery.DistinctType.WEAK;

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

    @Override public <R> BIt<R> execute(RowType<R> rowType, @Nullable Binding binding, boolean canDedup) {
        var sparql = this.sparql;
        if (binding != null) sparql = sparql.bound(binding);
        if (canDedup)        sparql = sparql.toDistinct(WEAK);
        return client.query(rowType, sparql);
    }

    @Override public boolean equals(Object o) {
        return o instanceof Query q && sparql.equals(q.sparql)
                && client.equals(q.client);
    }

    @Override public int hashCode() {
        return Objects.hash(sparql, client);
    }
}
