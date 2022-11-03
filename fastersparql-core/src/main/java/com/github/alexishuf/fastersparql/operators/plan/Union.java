package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.row.dedup.Dedup;
import com.github.alexishuf.fastersparql.client.model.row.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.client.model.row.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.operators.FSOpsProperties;
import com.github.alexishuf.fastersparql.operators.bit.DedupConcatBIt;
import com.github.alexishuf.fastersparql.operators.bit.DedupMergeBIt;
import com.github.alexishuf.fastersparql.operators.bit.MeteredConcatBIt;
import com.github.alexishuf.fastersparql.operators.bit.MeteredMergeBIt;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class Union<R, I> extends Plan<R, I> {
    public final int crossDedupCapacity;
    private Boolean singleEndpoint;

    public Union(List<? extends Plan<R, I>> operands, int crossDedupCapacity,
                 @Nullable Plan<R, I> unbound, @Nullable String name) {
        super(operands.get(0).rowType, operands, unbound, name);
        this.crossDedupCapacity = crossDedupCapacity;
    }

    public boolean singleEndpoint() {
        if (singleEndpoint != null)
            return singleEndpoint;
        String single = "";
        for (Plan<R, I> o : operands) {
            if (o instanceof Query<R,I> q) {
                if (single.isEmpty()) single = q.client().endpoint().uri();
                else if (!single.equals(q.client().endpoint().uri()))
                    return singleEndpoint = Boolean.FALSE;
            }
        }
        return singleEndpoint = Boolean.TRUE;
    }

    @Override public BIt<R> execute(boolean canDedup) {
        var sources = new ArrayList<BIt<R>>(operands.size());
        for (Plan<R, I> o : operands)
            sources.add(o.execute(canDedup));
        int dedupCapacity = crossDedupCapacity > 0 ? crossDedupCapacity
                          : FSOpsProperties.dedupCapacity();
        return dispatch(sources, canDedup, crossDedupCapacity > 0,
                        singleEndpoint(), dedupCapacity, this);
    }

    /**
     * Create a parallel or sequential metered {@link BIt} for {@code plan} that returns
     * the union of all {@code sources}, optionally de-duplicating the union result
     * or performing cross-source de-duplication.
     *
     * @param sources set of {@link BIt}s that will feed the union
     * @param canDedup whether the union results can be de-duplicated as an optimization
     * @param crossDedup whether a row output by a source {@code i} can be dropped if
     *                   already produced by a source {@code j}
     * @param sequential whether iteration of source {@code i} can only start after source
     *                   {@code i-1} has been exhausted
     * @param dedupCapacity if {@code canDedup} or {@code crossDedup}, keep at most this
     *                      number of rows, per source with the goal of de-duplicating rows.
     * @param plan The {@link Plan} to which the resulting {@link BIt} will correspond. This
     *             will be used to apply projections (if necessary) and to generate a
     *             {@link PlanMetrics} upon exhaustion of the union {@link BIt}.
     * @return a union {@link BIt} for all sources.
     */
    public static <R> BIt<R> dispatch(List<BIt<R>> sources, boolean canDedup,
                                      boolean crossDedup, boolean sequential,
                                      int dedupCapacity, Plan<R, ?> plan) {
        int totalCapacity = sources.size() * dedupCapacity;
        Dedup<R> dedup = null;
        if (canDedup)
            dedup = new WeakDedup<>(plan.rowType, totalCapacity);
        else if (crossDedup)
            dedup = new WeakCrossSourceDedup<>(plan.rowType, totalCapacity);
        if (sequential) {
            return dedup == null ? new MeteredConcatBIt<>(sources, plan)
                                 : new DedupConcatBIt<>(sources, plan, dedup);
        }
        return dedup == null ? new MeteredMergeBIt<>(sources, plan)
                             : new DedupMergeBIt<>(sources, plan, dedup);
    }

    @Override public String algebraName() {
        return crossDedupCapacity == 0 ? "Union" : "Union[crossDedup="+ crossDedupCapacity +']';
    }

    @Override
    public Plan<R, I> with(List<? extends Plan<R, I>> replacement, @Nullable Plan<R, I> unbound, @Nullable String name) {
        unbound = unbound == null ? this.unbound : unbound;
        name = name == null ? this.name : name;
        return new Union<>(replacement, crossDedupCapacity, unbound, name);
    }
}
