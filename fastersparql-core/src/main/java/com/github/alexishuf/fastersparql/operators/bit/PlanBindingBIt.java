package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.BindingBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.ItBindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.Query;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public final class PlanBindingBIt<B extends Batch<B>> extends BindingBIt<B> {
    private final boolean canDedup;

    public PlanBindingBIt(ItBindQuery<B> bindQuery, boolean canDedup, @Nullable Vars projection) {
        super(bindQuery, projection);
        this.canDedup = canDedup;
        if (bindQuery.query instanceof Plan p)
            scanClients(p, Objects.requireNonNull(guards));
    }

    public static void scanClients(Plan p, List<SparqlClient.Guard> guards) {
        int n = p.opCount();
        if (n == 0) {
            if (p instanceof Query q)
                guards.add(q.client.retain());
        } else {
            for (int i = 0; i < n; i++)
                scanClients(p.op(i), guards);
        }
    }

    @Override protected BIt<B> bind(BatchBinding binding) {
        return bindQuery.parsedQuery().bound(binding).execute(batchType, canDedup);
    }
}
