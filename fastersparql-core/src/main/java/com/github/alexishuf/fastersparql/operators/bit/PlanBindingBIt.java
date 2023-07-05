package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.BindingBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.BindQuery;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.Query;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class PlanBindingBIt<B extends Batch<B>> extends BindingBIt<B> {
    private final boolean canDedup;

    public PlanBindingBIt(BindQuery<B> bindQuery, boolean canDedup, @Nullable Vars projection) {
        super(bindQuery, projection);
        this.canDedup = canDedup;
        if (bindQuery.query instanceof Plan p)
            scanClients(p);
    }

    private void scanClients(Plan p) {
        int n = p.opCount();
        if (n == 0) {
            if (p instanceof Query q)
                addGuard(q.client.retain());
        } else {
            for (int i = 0; i < n; i++)
                scanClients(p.op(i));
        }
    }

    @Override protected BIt<B> bind(BatchBinding<B> binding) {
        return bindQuery.parsedQuery().bound(binding).execute(batchType, canDedup);
    }
}
