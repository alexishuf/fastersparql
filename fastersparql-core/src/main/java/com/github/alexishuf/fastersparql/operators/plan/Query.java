package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.util.Skip;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.sparql.SparqlQuery.DistinctType.STRONG;

public class Query<R, I> extends Plan<R, I> {
    private final SparqlQuery sparql;
    private final SparqlClient<R, I, ?> client;

    public Query(SparqlQuery sparql, SparqlClient<R, I, ?> client,
                 @Nullable Plan<R, I> unbound, @Nullable String name) {
        super(client.rowType(), List.of(), unbound, name);
        this.sparql = sparql;
        this.client = client;
    }

    public final SparqlQuery           query() { return sparql; }
    public final SparqlClient<R, I, ?> client() { return client; }

    @Override public String sparql() { return sparql.sparql(); }

    @Override public void groupGraphPatternInner(StringBuilder out, int indent) {
        if (sparql instanceof Plan<?,?> p)
            p.groupGraphPatternInner(out, indent);
        else
            out.append(sparql.sparql().replaceAll("\n", '\n'+" ".repeat(indent)));
    }

    @Override protected Vars computeVars(boolean all) {
        return all ? sparql.allVars() : sparql.publicVars();
    }

    @Override public String algebraName() {
        StringBuilder sb = new StringBuilder();
        sb.append("Query[").append(client.endpoint().uri()).append("](");
        String sparql = sparql();
        if (sparql.length() < 80) {
            return sb.append(sparql.replace("\n", "\\n")).append(')').toString();
        } else {
            sb.append('\n');
            for (int start = 0, i, len = sparql.length(); start < len; start = i+1) {
                i = Skip.skipUntil(sparql, start, sparql.length(), '\n');
                sb.append("  ").append(sparql, start, i).append('\n');
            }
            return sb.append(')').toString();
        }
    }

    @Override public BIt<R> execute(boolean canDedup) {
        return client.query(canDedup ? sparql.toDistinct(STRONG) : sparql);
    }

    @Override
    public Plan<R, I> with(List<? extends Plan<R, I>> replacement,
                           @Nullable Plan<R, I> unbound, @Nullable String name) {
        if (!replacement.isEmpty())
            throw new IllegalArgumentException("Expected no operands, got "+replacement.size());
        unbound = unbound == null ? this.unbound : unbound;
        name    = name    == null ? this.name    : name;
        return new Query<>(sparql, client, unbound, name);
    }

    @Override public Plan<R, I> bind(Binding binding) {
        SparqlQuery bound = sparql.bind(binding);
        return bound == sparql ? this : new Query<>(bound, client, this, name);
    }

    @Override public boolean equals(Object o) {
        return o instanceof Query<?, ?> q && sparql.equals(q.sparql)
                && client.equals(q.client)
                && super.equals(o);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), sparql, client);
    }
}
