package com.github.alexishuf.fastersparql.client.model.row;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;

public class RowHashSet<R> implements RowSet<R> {
    private final HashSet<Object> set = new HashSet<>();
    private final @Nullable RowOperations rowOps;

    public RowHashSet(RowOperations rowOps) {
        this.rowOps = rowOps.needsCustomHash() ? rowOps : null;
    }

    @Override public boolean add(@Nullable R row) {
        return set.add(rowOps == null ? row : new Adapter(rowOps, row));
    }

    @Override public boolean contains(@Nullable R row) {
        return set.contains(rowOps == null ? row : new Adapter(rowOps, row));
    }
}
