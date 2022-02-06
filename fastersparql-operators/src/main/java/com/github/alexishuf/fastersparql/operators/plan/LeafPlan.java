package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.util.List;
import java.util.Map;

@Getter @Accessors(fluent = true)
@EqualsAndHashCode @ToString
public class LeafPlan<R> implements Plan<R> {
    private final CharSequence query;
    private final SparqlClient<R, ?> client;
    private final SparqlConfiguration configuration;
    private @MonotonicNonNull List<String> publicVars;
    private @MonotonicNonNull List<String> allVars;

    public LeafPlan(CharSequence query, SparqlClient<R, ?> client) {
        this(query, client, SparqlConfiguration.EMPTY);
    }

    public LeafPlan(CharSequence query, SparqlClient<R, ?> client,
                    SparqlConfiguration configuration) {
        this.query = query.toString();
        this.client = client;
        this.configuration = configuration;
    }

    private LeafPlan(SparqlConfiguration config, SparqlClient<R, ?> client, CharSequence query) {
        this.query = query;
        this.client = client;
        this.configuration = config;
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
        return new LeafPlan<>(configuration, client, SparqlUtils.bind(query, var2ntValue));
    }
}
