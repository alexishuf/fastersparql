package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.ItBindQuery;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

public class EmptyBindingBIt<B extends Batch<B>> extends AbstractBIt<B> {
    private final ItBindQuery<B> bindQuery;

    public EmptyBindingBIt(ItBindQuery<B> bindQuery, @Nullable Vars projection) {
        super(bindQuery.batchType(),
              projection != null ? projection : bindQuery.resultVars());
        this.bindQuery = bindQuery;
    }

    @Override public @Nullable Orphan<B> nextBatch(@Nullable Orphan<B> offer) {
        long seq = 0;
        try (var g = new Guard.ItGuard<>(this, bindQuery.bindings)) {
            for (B b; (b=g.nextBatch()) != null; ) {
                for (int i = 0; i < b.totalRows(); i++)
                    bindQuery.emptyBinding(seq++);
            }
        }
        return null;
    }
}
