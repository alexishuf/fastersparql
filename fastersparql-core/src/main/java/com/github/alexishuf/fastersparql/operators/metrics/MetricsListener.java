package com.github.alexishuf.fastersparql.operators.metrics;

import java.util.function.Consumer;

@FunctionalInterface
public interface MetricsListener extends Consumer<Metrics> { }
