package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.util.SafeCloseable;

public interface SparqlServer extends SafeCloseable {
    String sparqlPath();
    int port();
}
