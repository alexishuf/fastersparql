package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItClosedException;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.operators.FilteringTransformBIt;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.dedup.Dedup;
import com.github.alexishuf.fastersparql.client.model.row.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.client.model.row.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.client.model.row.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.client.util.bind.BS;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.binding.RowBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.github.alexishuf.fastersparql.client.util.Merger.forProjection;
import static com.github.alexishuf.fastersparql.client.util.bind.BS.*;
import static com.github.alexishuf.fastersparql.operators.FSOpsProperties.dedupCapacity;
import static com.github.alexishuf.fastersparql.operators.FSOpsProperties.distinctCapacity;
import static java.lang.System.arraycopy;

@SuppressWarnings("unused")
public final class Modifier<R, I> extends Plan<R, I> {
    public final @Nullable Vars projection;
    public final int distinctCapacity;
    public final long offset, limit;
    public final List<Expr> filters;

    public Modifier(Plan<R, I> input, @Nullable Vars projection, int distinctCapacity,
                    long offset, long limit, List<Expr> filters,
                    @Nullable Plan<R, I> unbound, @Nullable String name) {
        super(input.rowType, List.of(input), unbound, name);
        this.projection = projection;
        this.distinctCapacity = distinctCapacity;
        this.offset = offset;
        this.limit = limit;
        this.filters = filters == null ? List.of() : filters;
    }

    public @Nullable Vars     projection() { return projection; }
    public int            distinctWindow() { return distinctCapacity; }
    public long                   offset() { return offset; }
    public long                    limit() { return limit; }
    public List<Expr>            filters() { return filters; }

    @Override protected Vars computeVars(boolean all) {
        Plan<R, I> in = operands.get(0);
        if (projection == null)
            return all ? in.allVars() : in.publicVars();
        return all ? in.allVars() : projection;
    }

    @Override public String algebraName() {
        var sb = new StringBuilder();
        if (offset > 0)
            sb.append("Offset[").append(offset).append("](");
        if (limit > 0 && limit < Long.MAX_VALUE)
            sb.append("Limit[").append(limit).append("](");
        if (distinctCapacity > 0) {
            sb.append("Distinct");
            if (distinctCapacity < Integer.MAX_VALUE)
                sb.append("[window=").append(distinctCapacity).append(']');
            sb.append('(');
        }
        if (projection != null)
            sb.append("Project").append(projection).append('(');
        if (!filters.isEmpty())
            sb.append("Filter").append(filters).append('(');
        return sb.toString();
    }

    @Override public String toString() {
        var sb = new StringBuilder();
        sb.append(algebraName()).append(operands.get(0));

        if (!filters.isEmpty())                  sb.append(')');
        if (distinctCapacity > 0)                sb.append(')');
        if (limit > 0 && limit < Long.MAX_VALUE) sb.append(')');
        if (offset > 0)                          sb.append(')');
        if (projection != null)                  sb.append(')');
        return sb.toString();
    }

    @Override
    public Plan<R, I> with(List<? extends Plan<R, I>> replacement, @Nullable Plan<R, I> unbound, @Nullable String name) {
        if (replacement.size() != 1)
            throw new IllegalArgumentException("Expected 1 operand, got "+replacement.size());
        unbound = unbound == null ? this.unbound : unbound;
        name = name == null ? this.name : name;
        return new Modifier<>(replacement.get(0), projection, distinctCapacity,
                              offset, limit, filters, unbound, name);
    }

    @Override public Plan<R, I> bind(Binding binding) {

        Plan<R, I> in = operands.get(0), bIn = in.bind(binding);
        Vars bProjection = projection == null ? null : projection.minus(binding.vars);

        List<Expr> bFilters = filters.isEmpty() ? filters : new ArrayList<>();

        boolean filterChange = false;
        for (Expr e : filters) {
            Expr bound = e.bind(binding);
            filterChange |= bound != e;
            bFilters.add(bound);
        }
        if (!filterChange)
            bFilters = filters;

        boolean change = bIn != in || bProjection != projection || filterChange;
        return change ? new Modifier<>(bIn, bProjection, distinctCapacity, offset,
                                       limit, bFilters, this, name)
                      : this;
    }

    @Override public BIt<R> execute(boolean canDedup) {
        boolean childDedup = canDedup || distinctCapacity > 0;
        BIt<R> in = operands.get(0).execute(childDedup);
        if (filters.isEmpty())
            return new NoFilterModifierBIt<>(in, this, canDedup);
        else if (distinctCapacity == 0 && !canDedup)
            return new NoDedupModifierBIt<>(in, this);
        return new GeneralModifierBIt<>(in, this, canDedup);
    }

    static abstract class ModifierBIt<R> extends FilteringTransformBIt<R, R> {
        private static final Logger log = LoggerFactory.getLogger(ModifierBIt.class);
        protected final Modifier<R, ?> plan;
        protected final @Nullable Merger<R, ?> filterSetProjector;
        protected final @Nullable Merger<R, ?> projector;
        protected final RowBinding<R, ?> binding;
        protected final boolean hasFilters, hasSlice;
        protected final long startNanos = System.nanoTime();
        protected final @Nullable Dedup<R> outerSet;
        protected final @Nullable WeakDedup<R> preFilterSet;
        protected long rows;
        protected int failures;
        protected long pendingSkips;


        public ModifierBIt(BIt<R> in, Modifier<R, ?> plan, boolean canDedup) {
            super(in, plan.rowType.rowClass, plan.publicVars());
            this.plan = plan;
            var inVars = in.vars();
            var rt = plan.rowType;
            hasFilters = !plan.filters.isEmpty();
            hasSlice = plan.offset > 0 || plan.limit != Long.MAX_VALUE;
            pendingSkips = plan.offset;
            if (plan.distinctCapacity > 0 || canDedup) {
                if (plan.distinctCapacity == Integer.MAX_VALUE) {
                    outerSet = new StrongDedup<>(rt, distinctCapacity());
                } else {
                    int c = plan.distinctCapacity > 0 ? plan.distinctCapacity : dedupCapacity();
                    outerSet = new WeakCrossSourceDedup<>(rt, c);
                }
                preFilterSet = hasFilters ? new WeakDedup<>(rt, 32) : null;
            } else {
                outerSet = preFilterSet = null;
            }
            if (plan.projection == null) {
                projector = filterSetProjector = null;
                binding = hasFilters ? new RowBinding<>(rt, inVars) : null;
            } else if (hasFilters) {
                int filterOnlyVars = 0; //vars in filters but not in plan.projection
                if (plan.distinctCapacity > 0) {
                    // only project before preFilterSet if we can remove some var
                    var beforeFilterVars = Vars.fromSet(plan.projection, inVars.size());
                    for (Expr e : plan.filters)
                        filterOnlyVars += Expr.addVars(beforeFilterVars, e);
                    int removed = beforeFilterVars.novelItems(inVars, 1);
                    filterSetProjector = removed == 0 ? null
                                    : forProjection(rt, beforeFilterVars, inVars);
                } else {
                    filterSetProjector = null;
                }
                binding = new RowBinding<>(rt, filterSetProjector == null ? inVars : filterSetProjector.outVars);
                projector = filterSetProjector == null || filterOnlyVars > 0
                          ? forProjection(rt, plan.projection, inVars)
                          : null;
            } else {
                projector = forProjection(rt, plan.projection, inVars);
                filterSetProjector = null;
                binding = null;
            }
        }

        @Override protected String toStringNoArgs() { return plan.name; }

        @Override protected void cleanup(Throwable error) {
            boolean cancel = BItClosedException.isClosedExceptionFor(error, delegate);
            if (error != null && !cancel)
                delegate.close();
            PlanMetrics.buildAndSend(plan, rows, startNanos, error, cancel);
        }

        /* --- --- --- helpers --- --- --- */

        /** Log only the first few failures during filter evaluation. */
        protected final void logFilterFailed(R row, Throwable t) {
            if (failures <= 5) {
                ++failures;
                String stopMsg = failures < 5
                               ? "" : ". Will stop reporting for this iterator instance";
                log.info("Failed to evaluate FILTER{} on {}: {}{}",
                         plan.filters, plan.rowType.toString(row), t.getMessage(), stopMsg);
            }
        }
    }

    /** Implements any possible {@link Modifier} */
    static class GeneralModifierBIt<R> extends ModifierBIt<R> {
        protected int[] include;

        public GeneralModifierBIt(BIt<R> in, Modifier<R, ?> plan, boolean canDedup) {
            super(in, plan, canDedup);
        }

        @Override protected Batch<R> process(Batch<R> b) {
            R[] a = b.array;
            include = init(include, b.size);
            set(include, 0, b.size);
            if (hasFilters) filter(a);
            if (outerSet != null) {
                if (projector == null) dedup(a, outerSet);
                else                   dedup(a, outerSet, projector);
            }
            if (pendingSkips > 0) offset();
            if (plan.limit < Long.MAX_VALUE) limit();
            if (projector == null || outerSet != null) b.size = condense(a);
            else                                       b.size = condense(a, projector);

            rows += b.size;
            return b;
        }

        private void filter(R[] a) {
            if (preFilterSet != null) {
                if (filterSetProjector == null) dedup(a, preFilterSet);
                else                            dedup(a, preFilterSet, filterSetProjector);
            }
            for (int i = nextSet(include, 0); i >= 0; i = nextSet(include, i+1)) {
                binding.row(a[i]);
                boolean ok = true;
                try {
                    for (Expr filter : plan.filters) {
                        ok = filter.evalAs(binding, Term.Lit.class).asBool();
                        if (!ok) break;
                    }
                } catch (Throwable t) {
                    logFilterFailed(a[i], t);
                }
                if (!ok)
                    include[i>>5] &= ~(1 << i);
            }
        }

        private void dedup(R[] a, Dedup<R> set) {
            for (int i = nextSet(include, 0); i >= 0; i = nextSet(include, i + 1)) {
                if (set.isDuplicate(a[i], 0))
                    include[i>>5] &= ~(1 << i);
            }
        }

        private void dedup(R[] a, Dedup<R> set, Merger<R, ?> projector) {
            for (int i = nextSet(include, 0); i >= 0; i = nextSet(include, i+1)) {
                if (set.isDuplicate(a[i], 0))
                    include[i>>5] &= ~(1 << i);
                else
                    a[i] = projector.merge(a[i], null);
            }
        }

        private int condense(R[] a) {
            int o = 0;
            for (int i = nextSet(include, 0); i >= 0; o++, i = nextSet(include, i+1)) {
                if (o != i) a[o] = a[i];
            }
            return o;
        }

        private int condense(R[] a, Merger<R, ?> projector) {
            int o = 0;
            for (int i = nextSet(include, 0); i >= 0; o++, i = nextSet(include, i+1))
                a[o] = projector.merge(a[i], null);
            return o;
        }

        private void offset() {
            int clearUntil = 0, i = nextSet(include, 0);
            while (i != -1 && pendingSkips > 0) {
                --pendingSkips;
                i = nextSet(include, (clearUntil = i)+1);
            }
            clear(include, 0, clearUntil+1);
        }

        private void limit() {
            long allowed = plan.limit - rows;
            if (allowed < (long) include.length <<5 && allowed <  BS.cardinality(include)) {
                int last = -1;
                for (int i = 0; i < allowed; i++)
                    last = nextSet(include, last+1);
                clear(include, last + 1, include.length << 5);
            }
        }
    }

    /** Implements a {@link Modifier} without de-duplication (FILTER/OFFSET/LIMIT/projection) */
    static class NoDedupModifierBIt<R> extends ModifierBIt<R> {
        public NoDedupModifierBIt(BIt<R> in, Modifier<R, ?> plan) {
            super(in, plan, false);
            assert outerSet == null && this.preFilterSet == null
                                    && this.filterSetProjector == null;
        }

        @Override protected Batch<R> process(Batch<R> b) {
            R[] a = b.array;
            if (hasFilters) {
                List<Expr> filters = plan.filters;
                int passed = 0;
                int maxPassed = (int)Math.min(b.size, pendingSkips + plan.limit-rows);
                batch: for (int i = 0, n = b.size; i < n && passed < maxPassed; i++) {
                    try {
                        binding.row(a[i]);
                        for (Expr expr : filters) {
                            if (!expr.evalAs(binding, Term.Lit.class).asBool())
                                continue batch;
                        }
                        if (passed != i) a[passed] = a[i];
                        ++passed;
                    } catch (Throwable t) {
                        logFilterFailed(a[i], t);
                    }
                }
                b.size = passed;
            }
            if (hasSlice) {
                int skip = 0;
                if (pendingSkips > 0) {
                    skip = (int) Math.min(pendingSkips, b.size);
                    pendingSkips -= skip;
                }
                b.size = (int)Math.min(plan.limit-rows, b.size-skip);
                if (b.size > 0)
                    arraycopy(a, skip, a, 0, b.size);
            }
            if (projector != null) {
                for (int i = 0, n = b.size; i < n; i++)
                    a[i] = projector.merge(a[i], null);
            }
            rows += b.size;
            return b;
        }
    }

    /** Implements a {@link Modifier} without FILTER clauses (DISTINCT/OFFSET/LIMIT/project) */
    static class NoFilterModifierBIt<R> extends ModifierBIt<R> {
        public NoFilterModifierBIt(BIt<R> in, Modifier<R, ?> plan, boolean canDedup) {
            super(in, plan, canDedup);
            assert !hasFilters && preFilterSet == null && filterSetProjector == null;
        }

        @Override protected Batch<R> process(Batch<R> b) {
            R[] a = b.array;
            if (outerSet != null)
                outerSet.filter(b, 0, projector);
            if (hasSlice) {
                int skip = (int) Math.min(pendingSkips, b.size);
                b.size = (int) Math.min(plan.limit - rows, b.size - skip);
                if (b.size > 0)
                    arraycopy(a, skip, a, 0, b.size);
            }
            if (outerSet == null && projector != null) {
                for (int i = 0, n = b.size; i < n; i++)
                    a[i] = projector.merge(a[i], null);
            }
            rows += b.size;
            return b;
        }
    }
}
