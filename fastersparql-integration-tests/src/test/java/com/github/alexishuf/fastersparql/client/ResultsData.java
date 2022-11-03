package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.ResultsChecker;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;

import java.util.List;

class ResultsData extends ResultsChecker {
    private final SparqlQuery sparql;

    public ResultsData(ResultsData other) {
        super(other);
        this.sparql = other.sparql;
    }

    public static ResultsData results(String sparql, boolean value) {
        return new ResultsData(sparql, value);
    }
    public static ResultsData results(String sparql, String... values) {
        return new ResultsData(sparql, values);
    }

    public ResultsData(String sparql, boolean value) {
        super(value);
        this.sparql = new SparqlQuery(PREFIX+sparql);
    }
    public ResultsData(String sparql, String... values) {
        super(new SparqlQuery(sparql).publicVars, values);
        this.sparql = new SparqlQuery(PREFIX+sparql);
    }

    public ResultsData(String sparql, List<String> vars, String... values) {
        super(vars, values);
        this.sparql = new SparqlQuery(PREFIX+sparql);
    }

    public SparqlQuery         sparql() { return sparql; }

    @Override public String toString() {
        return sparql.sparql.replace(PREFIX, "").replace("\n", "\\n");
    }
}
