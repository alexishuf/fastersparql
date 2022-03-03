package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Getter @Accessors(fluent = true)
@EqualsAndHashCode @ToString
public class LeafPlan<R> implements Plan<R> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final CharSequence query;
    private final SparqlClient<R, ?> client;
    private final SparqlConfiguration configuration;
    private final String name;
    private @MonotonicNonNull List<String> publicVars;
    private @MonotonicNonNull List<String> allVars;

    @Builder
    public LeafPlan(@lombok.NonNull CharSequence query,
                    @lombok.NonNull SparqlClient<R, ?> client,
                    @Nullable SparqlConfiguration configuration, @Nullable String name) {
        this.query = query.toString();
        this.client = client;
        this.configuration = configuration == null ? SparqlConfiguration.EMPTY : configuration;
        this.name = name == null ? "Query-"+nextId.getAndIncrement() : name;
    }

    private LeafPlan(@lombok.NonNull String name, @lombok.NonNull CharSequence query,
                     @lombok.NonNull SparqlClient<R, ?> client,
                     @lombok.NonNull SparqlConfiguration configuration) {
        this.query = query;
        this.client = client;
        this.configuration = configuration;
        this.name = name;
    }

    public static <T> LeafPlanBuilder<T> builder(SparqlClient<T, ?> client, CharSequence query) {
        return new LeafPlanBuilder<T>().client(client).query(query);
    }

    @Override public Class<? super R> rowClass() {
        return client.rowClass();
    }

    @Override public List<String> publicVars() {
        return publicVars == null ? (publicVars = SparqlUtils.publicVars(query)) : publicVars;
    }

    @Override public List<String> allVars() {
        return allVars == null ? (allVars = SparqlUtils.allVars(query)) : allVars;
    }

    @Override public Results<R> execute() {
        return client.query(query, configuration);
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        CharSequence bound = SparqlUtils.bind(query, var2ntValue);
        return new LeafPlan<>(name, bound, client, configuration);
    }
}
