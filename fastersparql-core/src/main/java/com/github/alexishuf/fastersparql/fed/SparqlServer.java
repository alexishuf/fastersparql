package com.github.alexishuf.fastersparql.fed;

public interface SparqlServer extends AutoCloseable  {
    String sparqlPath();
    int port();
    @Override void close();
}
