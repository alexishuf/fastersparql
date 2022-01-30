package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import lombok.Value;

import java.util.Map;

@Value
public class LeafPlan<R> implements Plan<R> {
    CharSequence query;
    SparqlClient<R, ?> client;
    SparqlConfiguration configuration;

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

    @Override public Results<R> execute() {
        return client.query(query, configuration);
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        return new LeafPlan<>(configuration, client, SparqlUtils.bind(query, var2ntValue));
    }
}
