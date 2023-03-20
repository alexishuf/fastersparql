package com.github.alexishuf.fastersparql.batch.type;

public abstract class ProjectionRowFilter<B extends Batch<B>> implements RowFilter<B> {
    @Override public boolean targetsProjection() { return true; }
}
