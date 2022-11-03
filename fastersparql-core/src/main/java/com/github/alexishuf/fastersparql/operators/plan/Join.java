package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public abstract class Join<R, I> extends Plan<R, I>{
    public Join(RowType<R, I> rowType,
                List<? extends Plan<R, I>> operands, @Nullable Plan<R, I> unbound,
                @Nullable String name) {
        super(rowType, operands, unbound, name);
    }
}
