package com.github.alexishuf.fastersparql.client.model.row;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.LinkedHashSet;

public class RowHashWindowSet<R> implements RowSet<R> {
    private final LinkedHashSet<Object> set = new LinkedHashSet<>();
    private final @Nullable RowOperations rowOps;
    private final int windowSize;

    public RowHashWindowSet(int windowSize, RowOperations rowOps) {
        this.windowSize = windowSize;
        this.rowOps = rowOps.needsCustomHash() ? rowOps : null;
    }

    @Override public boolean add(@Nullable R row) {
        if (set.add(rowOps == null ? row : new Adapter(rowOps, row))) {
            if (set.size() > windowSize) {
                Iterator<Object> it = set.iterator();
                assert it.hasNext();
                it.next();
                it.remove();
            }
            return true;
        }
        return false;
    }

    @Override public boolean contains(@Nullable R row) {
        return set.contains(rowOps == null ? row : new Adapter(rowOps, row));
    }
}
