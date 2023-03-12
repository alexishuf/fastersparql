package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.operators.FilteringTransformBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.model.row.dedup.Dedup;
import com.github.alexishuf.fastersparql.model.row.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.model.row.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.binding.RowBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.util.BS;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.FSProperties.dedupCapacity;
import static com.github.alexishuf.fastersparql.FSProperties.reducedCapacity;
import static com.github.alexishuf.fastersparql.batch.BItClosedAtException.isClosedFor;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.*;
import static com.github.alexishuf.fastersparql.util.BS.*;
import static java.lang.System.arraycopy;

@SuppressWarnings("unused")
public final class Modifier extends Plan {
    public  @Nullable Vars projection;
    public int distinctCapacity;
    public long offset, limit;
    public List<Expr> filters;

    public Modifier(Plan in, @Nullable Vars projection, int distinctCapacity,
                    long offset, long limit, List<Expr> filters) {
        super(Operator.MODIFIER);
        this.left = in;
        this.projection = projection;
        this.distinctCapacity = distinctCapacity;
        this.offset = offset;
        this.limit = limit;
        this.filters = filters == null ? List.of() : filters;
    }

    @Override public Modifier copy(@Nullable Plan[] ops) {
        Plan left = ops == null ? this.left : ops[0];
        return new Modifier(left, projection, distinctCapacity, offset, limit, filters);
    }

    public @Nullable Vars     projection() { return projection; }
    public int distinctCapacity() { return distinctCapacity; }
    public long                   offset() { return offset; }
    public long                    limit() { return limit; }
    public List<Expr>            filters() { return filters; }

    public boolean isNoOp() {
        //noinspection DataFlowIssue
        return (projection == null || projection.equals(left.publicVars()))
                && distinctCapacity == 0 && offset == 0 && limit == Long.MAX_VALUE
                && filters.isEmpty();
    }

    private static final byte[] OFFSET_LBRA = "Offset[".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LIMIT_LBRA = "Limit[".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DISTINCT = "Distinct".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PROJECT = "Project".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FILTER = "Filter".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LBRA_WINDOW = "[window=".getBytes(StandardCharsets.UTF_8);
    @Override public Rope algebraName() {
        var rb = new ByteRope();
        if (offset > 0)
            rb.append(OFFSET_LBRA).append(offset).append(']').append('(');
        if (limit > 0 && limit < Long.MAX_VALUE)
            rb.append(LIMIT_LBRA).append(limit).append(']').append('(');
        if (distinctCapacity > 0) {
            rb.append(DISTINCT);
            if (distinctCapacity < Integer.MAX_VALUE)
                rb.append(LBRA_WINDOW).append(distinctCapacity).append(']');
            rb.append('(');
        }
        if (projection != null)
            rb.append(PROJECT).append(projection).append('(');
        if (!filters.isEmpty())
            rb.append(FILTER).append(filters).append('(');
        return rb;
    }

    @Override public boolean equals(Object o) {
        return o instanceof Modifier m
                && Objects.equals(m.projection,       projection)
                && Objects.equals(m.distinctCapacity, distinctCapacity)
                && Objects.equals(m.offset,           offset)
                && Objects.equals(m.limit,            limit)
                && Objects.equals(m.filters,          filters);
    }

    @Override public int hashCode() {
        return Objects.hash(type, left, projection, distinctCapacity, offset, limit, filters);
    }

    @Override public String toString() {
        var sb = new StringBuilder();
        sb.append(algebraName()).append(left);

        if (!filters.isEmpty())                  sb.append(')');
        if (distinctCapacity > 0)                sb.append(')');
        if (limit > 0 && limit < Long.MAX_VALUE) sb.append(')');
        if (offset > 0)                          sb.append(')');
        if (projection != null)                  sb.append(')');
        return sb.toString();
    }

    @Override public Rope sparql() {
        var sb = new ByteRope(256);
        if (projection != null && projection.isEmpty() && limit == 1 && offset == 0) {
            groupGraphPattern(sb.append(ASK_u8).append(' '), 0, PrefixAssigner.NOP);
        } else {
            sb.append(SELECT_u8).append(' ');
            if      (distinctCapacity > reducedCapacity()) sb.append(DISTINCT_u8).append(' ');
            else if (distinctCapacity > 0)                 sb.append( REDUCED_u8).append(' ');

            if (projection != null) {
                for (var s : projection) sb.append('?').append(s).append(' ');
                sb.unAppend(1);
            } else {
                sb.append('*');
            }

            groupGraphPattern(sb, 0, PrefixAssigner.NOP);
            if (offset > 0)             sb.append(' ').append(OFFSET_u8).append(' ').append(offset);
            if (limit < Long.MAX_VALUE) sb.append(' ').append( LIMIT_u8).append(' ').append(limit);
        }
        return sb;
    }

    @Override
    public <R> BIt<R> execute(RowType<R> rt, @Nullable Binding binding, boolean canDedup) {
        boolean childDedup = canDedup || distinctCapacity > 0;
        //noinspection DataFlowIssue
        BIt<R> in = left.execute(rt, binding, childDedup);
        List<Expr> filters = this.filters;
        if (filters.isEmpty())
            return new NoFilterModifierBIt<>(in, this, canDedup);
        if (binding != null) {
            List<Expr> bFilters = null;
            for (int i = 0, n = filters.size(); i < n; i++) {
                Expr e = filters.get(i), b = e.bound(binding);
                if (b != e)
                    (bFilters == null ? bFilters = new ArrayList<>(filters) : bFilters).set(i, b);
            }
            if (bFilters != null)
                filters = bFilters;
        }
        if (distinctCapacity == 0 && !canDedup)
            return new NoDedupModifierBIt<>(in, this, filters);
        return new GeneralModifierBIt<>(in, this, filters, canDedup);
    }

    static abstract class ModifierBIt<R> extends FilteringTransformBIt<R, R> {
        private static final Logger log = LoggerFactory.getLogger(ModifierBIt.class);
        protected final Modifier plan;
        protected final List<Expr> filters;
        protected final @Nullable Metrics metrics;
        protected final @Nullable RowType<R>.Merger filterSetProjector;
        protected final @Nullable RowType<R>.Merger projector;
        protected final RowBinding<R> binding;
        protected final boolean hasFilters, hasSlice;
        protected final long startNanos = System.nanoTime();
        protected final @Nullable Dedup<R> outerSet;
        protected final @Nullable WeakDedup<R> preFilterSet;
        protected int failures;
        protected long pendingSkips, rows;


        public ModifierBIt(BIt<R> in, Modifier plan, List<Expr> filters, boolean canDedup) {
            super(in, in.rowType(), plan.publicVars());
            this.filters = filters;
            this.metrics = Metrics.createIf(this.plan = plan);
            var inVars = in.vars();
            hasFilters = !filters.isEmpty();
            hasSlice = plan.offset > 0 || plan.limit != Long.MAX_VALUE;
            pendingSkips = plan.offset;
            if (plan.distinctCapacity > 0 || canDedup) {
                if (plan.distinctCapacity == Integer.MAX_VALUE) {
                    int cap = FSProperties.distinctCapacity();
                    outerSet = new StrongDedup<>(rowType, Math.max(256, cap>>8), cap);
                } else {
                    int c = plan.distinctCapacity > 0 ? plan.distinctCapacity : dedupCapacity();
                    outerSet = new WeakDedup<>(rowType, c);
                }
                preFilterSet = hasFilters ? new WeakDedup<>(rowType, 32) : null;
            } else {
                outerSet = preFilterSet = null;
            }
            if (plan.projection == null) {
                projector = filterSetProjector = null;
                binding = hasFilters ? new RowBinding<>(rowType, inVars) : null;
            } else if (hasFilters) {
                int filterOnlyVars = 0; //vars in filters but not in plan.projection
                if (plan.distinctCapacity > 0) {
                    // only project before preFilterSet if we can remove some var
                    var beforeFilterVars = Vars.fromSet(plan.projection, Math.max(10, inVars.size()));
                    for (Expr e : filters)
                        beforeFilterVars.addAll(e.vars());
                    filterOnlyVars = beforeFilterVars.size() - plan.projection.size();
                    int removed = beforeFilterVars.novelItems(inVars, 1);
                    filterSetProjector = removed == 0 ? null
                                    : rowType.projector(beforeFilterVars, inVars);
                } else {
                    filterSetProjector = null;
                }
                binding = new RowBinding<>(rowType, filterSetProjector == null ? inVars : filterSetProjector.outVars);
                projector = filterSetProjector == null || filterOnlyVars > 0
                          ? rowType.projector(plan.projection, inVars)
                          : null;
            } else {
                projector = rowType.projector(plan.projection, inVars);
                filterSetProjector = null;
                binding = null;
            }
        }

        @Override protected void cleanup(Throwable error) {
            if (error != null)
                delegate.close();
            if (metrics != null)
                metrics.complete(error, isClosedFor(error, this)).deliver();
        }

        /* --- --- --- helpers --- --- --- */

        /** Log only the first few failures during filter evaluation. */
        protected final void logFilterFailed(R row, Throwable t) {
            if (failures <= 5) {
                ++failures;
                String stopMsg = failures < 5
                               ? "" : ". Will stop reporting for this iterator instance";
                log.info("Failed to evaluate FILTER{} on {}: {}{}",
                         filters, rowType.toString(row), t.getMessage(), stopMsg);
            }
        }
    }

    /** Implements any possible {@link Modifier} */
    static class GeneralModifierBIt<R> extends ModifierBIt<R> {
        protected int[] include;

        public GeneralModifierBIt(BIt<R> in, Modifier plan, List<Expr> filters, boolean canDedup) {
            super(in, plan, filters, canDedup);
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
            if (metrics != null) metrics.rowsEmitted(b.size);
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
                    for (Expr filter : filters) {
                        if (!(ok = filter.eval(binding).asBool())) break;
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

        private void dedup(R[] a, Dedup<R> set, RowType<R>.Merger projector) {
            for (int i = nextSet(include, 0); i >= 0; i = nextSet(include, i+1)) {
                if (set.isDuplicate(a[i], 0))
                    include[i>>5] &= ~(1 << i);
                else
                    a[i] = projector.projectInPlace(a[i]);
            }
        }

        private int condense(R[] a) {
            int o = 0;
            for (int i = nextSet(include, 0); i >= 0; o++, i = nextSet(include, i+1)) {
                if (o != i) a[o] = a[i];
            }
            return o;
        }

        private int condense(R[] a, RowType<R>.Merger projector) {
            int o = 0;
            for (int i = nextSet(include, 0); i >= 0; o++, i = nextSet(include, i+1))
                a[o] = projector.projectInPlace(a[i]);
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

        public NoDedupModifierBIt(BIt<R> in, Modifier plan, List<Expr> filters) {
            super(in, plan, filters, false);
            assert outerSet == null && this.preFilterSet == null
                                    && this.filterSetProjector == null;
        }

        @Override protected Batch<R> process(Batch<R> b) {
            R[] a = b.array;
            if (hasFilters) {
                List<Expr> filters = this.filters;
                int passed = 0;
                int maxPassed = (int)Math.min(b.size, pendingSkips + plan.limit-rows);
                batch: for (int i = 0, n = b.size; i < n && passed < maxPassed; i++) {
                    try {
                        binding.row(a[i]);
                        for (Expr expr : filters) {
                            if (!expr.eval(binding).asBool()) continue batch;
                        }
                        a[passed++] = a[i];
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
                    a[i] = projector.projectInPlace(a[i]);
            }
            if (metrics != null) metrics.rowsEmitted(b.size);
            rows += b.size;
            return b;
        }
    }

    /** Implements a {@link Modifier} without FILTER clauses (DISTINCT/OFFSET/LIMIT/project) */
    static class NoFilterModifierBIt<R> extends ModifierBIt<R> {
        public NoFilterModifierBIt(BIt<R> in, Modifier plan, boolean canDedup) {
            super(in, plan, List.of(), canDedup);
            assert !hasFilters && preFilterSet == null && filterSetProjector == null;
        }

        @Override protected Batch<R> process(Batch<R> b) {
            R[] a = b.array;
            if (outerSet != null)
                outerSet.filter(b, 0, projector);
            if (hasSlice) {
                long skip = pendingSkips;
                if (skip > 0) {
                    skip = Math.min(skip, b.size);
                    pendingSkips -= skip;
                }
                b.size = (int) Math.min(plan.limit - rows, b.size - skip);
                if (b.size > 0)
                    arraycopy(a, (int)skip, a, 0, b.size);
            }
            if (outerSet == null && projector != null) {
                for (int i = 0, n = b.size; i < n; i++)
                    a[i] = projector.projectInPlace(a[i]);
            }
            if (metrics != null) metrics.rowsEmitted(b.size);
            rows += b.size;
            return b;
        }
    }
}
