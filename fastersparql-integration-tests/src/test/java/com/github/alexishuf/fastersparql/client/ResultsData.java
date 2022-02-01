package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.ResultsChecker;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.function.Consumer;

@EqualsAndHashCode(callSuper = true)
@ToString
@Getter @Setter @Accessors(fluent = true, chain = true)
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

    public ResultsData with(Consumer<SparqlConfiguration.SparqlConfigurationBuilder> configurator) {
        SparqlConfiguration.SparqlConfigurationBuilder builder = config.toBuilder();
        configurator.accept(builder);
        return new ResultsData(this).config(builder.build());
    }
}
