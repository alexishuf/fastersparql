package com.github.alexishuf.fastersparql.client.model.row;

import org.checkerframework.checker.nullness.qual.Nullable;

public interface RowSet<R> {
    class Adapter {
        private final RowOperations rowOps;
        private final Object row;

        public Adapter(RowOperations rowOps, Object row) {
            this.rowOps = rowOps;
            this.row = row;
        }

        @Override public boolean equals(Object o) {
            return o instanceof Adapter && rowOps.equalsSameVars(row, ((Adapter)o).row);
        }
        @Override public int    hashCode() { return rowOps.hash(row); }
        @Override public String toString() { return rowOps.toString(row); }
    }

    boolean add(@Nullable R row);
    boolean contains(@Nullable R row);
}
