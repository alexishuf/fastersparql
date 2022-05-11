package com.github.alexishuf.fastersparql.client.util.bind;

import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;

import java.util.List;

public interface Binder<R> {
    FSPublisher<R> bind(R row);
    List<String> resultVars();
    Binder<R> copyIfNotShareable();
}
