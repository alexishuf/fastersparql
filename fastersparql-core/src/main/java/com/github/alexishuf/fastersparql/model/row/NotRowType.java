package com.github.alexishuf.fastersparql.model.row;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class NotRowType<R> extends RowType<R> {
    private static final Map<Class<?>, NotRowType<?>> CACHE = new ConcurrentHashMap<>();
    public static final NotRowType<byte[]> BYTES = get(byte[].class);
    public static final NotRowType<Integer> INTEGER = get(Integer.class);
    @SuppressWarnings("unchecked")
    public static final NotRowType<List<Integer>> INTEGER_LIST = get((Class<List<Integer>>)(Object) List.class);

    private final Builder<R> builder;

    private NotRowType(Class<R> rowClass) {
        super(rowClass, Object.class);
        builder = new FailBuilder();
    }

    public static <R> NotRowType<R> get(Class<R> rowClass) {//noinspection unchecked
        return (NotRowType<R>) CACHE.computeIfAbsent(rowClass, NotRowType::new);
    }

    private class FailBuilder implements Builder<R> {
        @Override public boolean isEmpty() { return true; }

        @Override public @This Builder<R> set(int column, @Nullable Term term) {
            throw new UnsupportedOperationException(NotRowType.this.toString());
        }

        @Override public @This Builder<R> set(int column, R inRow, int inCol) {
            throw new UnsupportedOperationException(NotRowType.this.toString());
        }

        @Override public R build() {
            throw new UnsupportedOperationException(NotRowType.this.toString());
        }
    }

    @Override public Builder<R> builder(int columns) { return builder; }

    @Override public R empty() { return null; }
}
