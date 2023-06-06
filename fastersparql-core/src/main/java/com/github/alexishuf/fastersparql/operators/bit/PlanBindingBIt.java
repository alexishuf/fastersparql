package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.BindingBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.BindQuery;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class PlanBindingBIt<B extends Batch<B>> extends BindingBIt<B> {
    private final boolean canDedup;

    public PlanBindingBIt(BindQuery<B> bindQuery, boolean canDedup, @Nullable Vars projection) {
        super(bindQuery, projection);
        this.canDedup = canDedup;
    }

    @Override protected BIt<B> bind(BatchBinding<B> binding) {
        return bindQuery.parsedQuery().bound(binding).execute(batchType, canDedup);
    }
}
