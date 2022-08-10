package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.ResultsChecker;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

class ResultsData extends ResultsChecker {
    private SparqlConfiguration config = SparqlConfiguration.EMPTY;
    private String sparql;

    public ResultsData(ResultsData other) {
        super(other);
        this.sparql = other.sparql;
        this.config = other.config;
    }

    public static ResultsData results(String sparql, boolean value) {
        return new ResultsData(sparql, value);
    }
    public static ResultsData results(String sparql, String... values) {
        return new ResultsData(sparql, values);
    }

    public ResultsData(String sparql, boolean value) {
        super(value);
        this.sparql = PREFIX+sparql;
    }
    public ResultsData(String sparql, String... values) {
        super(SparqlUtils.publicVars(sparql), values);
        this.sparql = PREFIX+sparql;
    }

    public ResultsData(String sparql, List<String> vars, String... values) {
        super(vars, values);
        this.sparql = PREFIX+sparql;
    }

    public SparqlConfiguration config() { return config; }
    public String              sparql() { return sparql; }

    public ResultsData config(SparqlConfiguration value) { config = value; return this; }
    public ResultsData sparql(String value)              { sparql = value; return this; }


    public ResultsData with(Consumer<SparqlConfiguration.Builder> configurator) {
        SparqlConfiguration.Builder builder = config.toBuilder();
        configurator.accept(builder);
        return new ResultsData(this).config(builder.build());
    }

    public boolean isWs() {
        return config.methods().equals(Collections.singletonList(SparqlMethod.WS));
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ResultsData)) return false;
        ResultsData that = (ResultsData) o;
        return config.equals(that.config) && sparql.equals(that.sparql);
    }

    @Override public int hashCode() {
        return Objects.hash(config, sparql);
    }
}
