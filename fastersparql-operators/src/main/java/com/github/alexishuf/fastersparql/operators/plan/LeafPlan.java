package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.util.CSUtils;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;

public class LeafPlan<R> extends AbstractPlan<R, LeafPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final CharSequence query;
    private final SparqlClient<R, ?> client;
    private final SparqlConfiguration configuration;
    private @MonotonicNonNull List<String> publicVars;
    private @MonotonicNonNull List<String> allVars;

    @SuppressWarnings("unused")
    public static final class Builder<T> {
        private CharSequence query;
        private SparqlClient<T, ?> client;
        private @Nullable SparqlConfiguration configuration;
        private @Nullable LeafPlan<T> parent;
        private @Nullable String name;

        public Builder(CharSequence query, SparqlClient<T, ?> client) {
            this.query = query;
            this.client = client;
        }

        public Builder<T> query(CharSequence value)                          { query = value; return this; }
        public Builder<T> client(SparqlClient<T, ?> value)                   { client = value; return this; }
        public Builder<T> configuration(@Nullable SparqlConfiguration value) { configuration = value; return this; }
        public Builder<T> parent(@Nullable LeafPlan<T> value)                { parent = value; return this; }
        public Builder<T> name(@Nullable String value)                       { name = value; return this; }

        public LeafPlan<T> build() {
            return new LeafPlan<>(query, client, configuration, parent, name);
        }
    }

    public static <T> Builder<T> builder(SparqlClient<T, ?> client, CharSequence query) {
        return new Builder<>(query, client);
    }

    public LeafPlan(CharSequence query,  SparqlClient<R, ?> client,
                    @Nullable SparqlConfiguration configuration, @Nullable LeafPlan<R> parent,
                    @Nullable String name) {
        super(client.rowClass(), emptyList(),
              name == null ? "Query-"+nextId.getAndIncrement() : name, parent);
        this.query = query.toString();
        this.client = client;
        this.configuration = configuration == null ? SparqlConfiguration.EMPTY : configuration;
    }

    private LeafPlan(LeafPlan<R> parent, CharSequence query, SparqlClient<R, ?> client,
                     SparqlConfiguration configuration) {
        super(client.rowClass(), emptyList(), parent.name, parent);
        this.query = query;
        this.client = client;
        this.configuration = configuration;
    }

    public CharSequence        query()         { return query; }
    public SparqlClient<R, ?>  client()        { return client; }
    public SparqlConfiguration configuration() { return configuration; }

    @Override public List<String> publicVars() {
        if (publicVars == null)
            publicVars = SparqlUtils.publicVars(query);
        return publicVars;
    }

    @Override public List<String> allVars() {
        if (allVars == null)
            allVars = SparqlUtils.allVars(query);
        return allVars;
    }


    @Override protected String algebraName() {
        StringBuilder sb = new StringBuilder();
        sb.append("Query[").append(client.endpoint().uri()).append("](");
        if (query.length() < 80) {
            return sb.append(query.toString().replace("\n", "\\n")).append(')').toString();
        } else {
            sb.append('\n');
            for (int start = 0, i, len = query.length(); start < len; start = i+1) {
                i = CSUtils.skipUntil(query, start, '\n');
                sb.append("  ").append(query, start, i).append('\n');
            }
            return sb.append(')').toString();
        }
    }

    @Override public Results<R> execute() {
        return client.query(query, configuration);
    }

    @Override public Plan<R> bind(Binding binding) {
        CharSequence bound = SparqlUtils.bind(query, binding);
        return new LeafPlan<>(this, bound, client, configuration);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LeafPlan)) return false;
        if (!super.equals(o)) return false;
        LeafPlan<?> leafPlan = (LeafPlan<?>) o;
        return query.equals(leafPlan.query) && client.equals(leafPlan.client) && configuration.equals(leafPlan.configuration);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), query, client, configuration);
    }
}
