package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import lombok.Value;

import java.util.Map;

@Value
public class LeafPlan<R> implements Plan<R> {
    String query;
    SparqlClient<R, ?> client;
    SparqlConfiguration configuration;

    @Override public Results<R> execute() {
        return client.query(query, configuration);
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
