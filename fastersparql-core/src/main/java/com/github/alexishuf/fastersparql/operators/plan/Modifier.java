package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.operators.ProcessorBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.RowFilter;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.ExprEvaluator;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.FSProperties.*;
import static com.github.alexishuf.fastersparql.batch.dedup.Dedup.strongUntil;
import static com.github.alexishuf.fastersparql.batch.dedup.Dedup.weak;
import static com.github.alexishuf.fastersparql.sparql.DistinctType.STRONG;
import static com.github.alexishuf.fastersparql.sparql.DistinctType.WEAK;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.*;

@SuppressWarnings("unused")
public final class Modifier extends Plan {
    private static final Logger log = LoggerFactory.getLogger(Modifier.class);

    public @Nullable Vars projection;
    public @Nullable DistinctType distinct;
    public long offset, limit;
    public List<Expr> filters;

    public Modifier(Plan in, @Nullable Vars projection, @Nullable DistinctType distinct,
                    long offset, long limit, List<Expr> filters) {
        super(Operator.MODIFIER);
        this.left = in;
        this.projection = projection;
        this.distinct = distinct;
        this.offset = offset;
        this.limit = limit;
        this.filters = filters == null ? List.of() : filters;
    }

    @Override public Modifier copy(@Nullable Plan[] ops) {
        Plan left = ops == null ? this.left : ops[0];
        return new Modifier(left, projection, distinct, offset, limit, filters);
    }

    public @Nullable Vars       projection() { return projection; }
    public @Nullable DistinctType distinct() { return distinct; }
    public long                     offset() { return offset; }
    public long                      limit() { return limit; }
    public List<Expr>              filters() { return filters; }

    public boolean isNoOp() {
        //noinspection DataFlowIssue
        return (projection == null || projection.equals(left.publicVars()))
                && distinct == null && offset == 0 && limit == Long.MAX_VALUE
                && filters.isEmpty();
    }

    private static final byte[] OFFSET_LBRA = "Offset[".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LIMIT_LBRA = "Limit[".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DISTINCT = "Distinct".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PROJECT = "Project".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FILTER = "Filter".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LBRA_WINDOW = "[window=".getBytes(StandardCharsets.UTF_8);

    @Override public void algebraName(MutableRope dst) {
        if (offset > 0)
            dst.append(OFFSET_LBRA).append(offset).append(']').append('(');
        if (limit > 0 && limit < Long.MAX_VALUE)
            dst.append(LIMIT_LBRA).append(limit).append(']').append('(');
        if (distinct != null) {
            dst.append(distinct.sparql());
            dst.append('(');
        }
        if (projection != null)
            dst.append(PROJECT).append(projection).append('(');
        if (!filters.isEmpty())
            dst.append(FILTER).append(filters).append('(');
    }

    @Override public boolean equals(Object o) {
        return o instanceof Modifier m
                && Objects.equals(m.projection, projection)
                && Objects.equals(m.distinct,   distinct)
                && Objects.equals(m.offset,     offset)
                && Objects.equals(m.limit,      limit)
                && Objects.equals(m.filters,    filters);
    }

    @Override public int hashCode() {
        return Objects.hash(type, left, projection, distinct, offset, limit, filters);
    }

    @Override public String toString() {
        try (var sb = PooledMutableRope.get()) {
            algebraName(sb);
            sb.append(left);
            if (!filters.isEmpty())                   sb.append(')');
            if (distinct != null)                     sb.append(')');
            if (limit  > 0 && limit < Long.MAX_VALUE) sb.append(')');
            if (offset > 0)                           sb.append(')');
            if (projection != null)                   sb.append(')');

            return sb.toString();
        }
    }

    @Override public SegmentRope sparql() {
        try (var sb = PooledMutableRope.getWithCapacity(256)) {
            if (isAsk()) {
                groupGraphPattern(sb.append(ASK_u8).append(' '), 0, PrefixAssigner.NOP);
            } else {
                sb.append(SELECT_u8).append(' ');
                switch (distinct) {
                    case STRONG -> sb.append(DISTINCT_u8).append(' ');
                    case REDUCED -> sb.append(REDUCED_u8).append(' ');
                    case WEAK -> sb.append(PRUNED_u8).append(' ');
                    case null -> {
                    }
                }
                if (projection != null) {
                    for (var s : projection) sb.append('?').append(s).append(' ');
                    sb.unAppend(1);
                } else {
                    sb.append('*');
                }

                groupGraphPattern(sb, 0, PrefixAssigner.NOP);
                if (offset > 0) sb.append(' ').append(OFFSET_u8).append(' ').append(offset);
                if (limit < Long.MAX_VALUE)
                    sb.append(' ').append(LIMIT_u8).append(' ').append(limit);
            }
            return FinalSegmentRope.asFinal(sb);
        }
    }

    List<Expr> boundFilters(Binding binding) {
        List<Expr> filters = this.filters, boundFilters = null;
        for (int i = 0, n = filters.size(); i < n; i++) {
            Expr e = filters.get(i), b = e.bound(binding);
            if (e == b) continue;
            if (boundFilters == null) boundFilters = new ArrayList<>(filters);
            boundFilters.set(i, b);
        }
        return boundFilters == null ? filters : boundFilters;
    }

    @Override
    public <B extends Batch<B>>
    BIt<B> execute(BatchType<B> bt, @Nullable Binding binding, boolean weakDedup) {
        BIt<B> in = left().execute(bt, binding,
                (weakDedup && offset <= 0) || distinct != null);
        return executeFor(in, binding, weakDedup && distinct == null);
    }

    @Override
    public <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    doEmit(BatchType<B> type, Vars rebindHint, boolean weakDedup) {
        boolean weakDedupIn = weakDedup || (distinct != null && opportunisticDedup());
        var in = left().emit(type, rebindHint, weakDedupIn);
        Vars inVars = Emitter.peekVars(in);
        var pDedup = DistinctType.compareTo(distinct, weakDedupIn ? WEAK : null) > 0
                  || projection != null && !projection.equals(inVars)
                   ? distinct : null;
        var p = processorFor(type, inVars, null, pDedup);
        if (p == null)
            return in;
        return p.takeOwnership(this).subscribeTo(in).releaseOwnership(this);
    }

    public <B extends Batch<B>>
    BIt<B> executeFor(BIt<B> in, @Nullable Binding binding, boolean weakDedup) {
        var distinct = weakDedup ? WEAK : this.distinct;
        var processor = processorFor(in.batchType(), in.vars(), binding, distinct);
        if (processor == null) return in;
        return new ProcessorBIt<>(in, processor, Metrics.createIf(this));
    }

    public <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    processed(Orphan<? extends Emitter<B, ?>> in) {
        var p = processorFor(Emitter.peekBatchType(in), Emitter.peekVars(in), null, distinct);
        if (p == null)
            return in;
        return p.takeOwnership(this).subscribeTo(in).releaseOwnership(this);
    }

    public <B extends Batch<B>> Orphan<? extends BatchProcessor<B, ?>>
    processorFor(BatchType<B> bt, Vars inVars, @Nullable Binding binding, DistinctType distinct) {
        Vars outVars = projection == null ? inVars : projection;
        if (binding != null)
            outVars = outVars.minus(binding.vars());
        List<Expr> filters = binding == null ? this.filters : boundFilters(binding);
        int cols = outVars.size();
        long limit = this.limit;

        Orphan<? extends Dedup<B, ?>> dedup = null;
        if      (cols == 0)                               limit = distinct != null ? 1 : limit;
        else if (distinct == STRONG && !weakenDistinct()) dedup = strongUntil(bt, distinctCapacity(), cols);
        else if (distinct != null)                        dedup = weak(bt, cols, distinct);

        Orphan<? extends BatchProcessor<B, ?>> processor;
        boolean slice = limit < Long.MAX_VALUE || offset > 0;
        if (!filters.isEmpty()) {
            Orphan<? extends RowFilter<B, ?>> rf;
            if (slice && dedup == null)
                rf = new SlicingFiltering.Concrete<>(offset, limit, bt, inVars, filters);
            else
                rf = new Filtering.Concrete<>(bt, inVars, filters);
            var filter = bt.filter(outVars, inVars, rf);
            processor = filter;
            if (dedup != null) {
                Orphan<? extends RowFilter<B, ?>> dedupRF;
                if (slice)
                    dedupRF = new SlicingDedup.Concrete<>(offset, limit, dedup);
                else
                    dedupRF = dedup;
                processor = bt.filter(outVars, dedupRF, filter);
            }
        } else if (dedup == null) {
            processor = slice ? bt.filter(outVars, inVars, new Slicing.Concrete<>(offset, limit))
                              : bt.projector(outVars, inVars);
        } else {
            var rf = slice ? new SlicingDedup.Concrete<>(offset, limit, dedup) : dedup;
            processor = bt.filter(outVars, inVars, rf);
        }
        return processor;
    }


    /* --- --- --- RowFilter implementations --- --- --- */

    private static long upstreamRequestLimit(long offset, long limit) {
        long sum = offset + limit;
        return sum < 0 ? Long.MAX_VALUE : sum;
    }

    private static abstract sealed class Slicing<B extends Batch<B>>
            extends AbstractOwned<Slicing<B>>
            implements RowFilter<B, Slicing<B>> {
        private final long offset, limit;
        private long skip, allowed;

        public Slicing(long offset, long limit) {
            skip = this.offset = offset;
            allowed = this.limit = limit;
        }

        @Override public @Nullable Slicing<B> recycle(Object currentOwner) {
            return internalMarkGarbage(currentOwner);
        }

        private static final class Concrete<B extends Batch<B>>
                extends Slicing<B> implements Orphan<Slicing<B>> {
            public Concrete(long offset, long limit) {super(offset, limit);}
            @Override public Slicing<B> takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public long upstreamRequestLimit() {
            return Modifier.upstreamRequestLimit(skip, allowed);
        }
        @Override public void rebind(BatchBinding binding) throws RebindException {
            skip = offset;
            allowed = limit;
        }

        @Override public String toString() {
            var sb = new StringBuilder();
            if (offset > 0) sb.append("OFFSET ").append(offset);
            if (limit != Long.MAX_VALUE) sb.append("LIMIT ").append(limit);
            return sb.toString();
        }

        @Override public boolean isNoOp() { return skip <= 0 && allowed == Long.MAX_VALUE; }

        @Override public Decision drop(B batch, int row) {
            if (skip > 0) {
                --skip;
                return Decision.DROP;
            } else if (allowed == 0) {
                return Decision.TERMINATE;
            }
            if (allowed != Long.MAX_VALUE)
                --allowed;
            return Decision.KEEP;
        }
    }

    private static abstract sealed class SlicingDedup<B extends Batch<B>>
            extends AbstractOwned<SlicingDedup<B>>
            implements RowFilter<B, SlicingDedup<B>> {
        private final Dedup<B, ?> dedup;
        private final long offset, limit;
        private long skip, allowed;

        public SlicingDedup(long offset, long limit, Orphan<? extends Dedup<B, ?>> dedup) {
            this.dedup = dedup.takeOwnership(this);
            skip = this.offset = offset;
            allowed = this.limit = limit;
        }

        @Override public @Nullable SlicingDedup<B> recycle(Object currentOwner) {
            internalMarkRecycled(currentOwner);
            dedup.recycle(this);
            return null;
        }

        private static final class Concrete<B extends Batch<B>> extends SlicingDedup<B>
                implements Orphan<SlicingDedup<B>> {
            public Concrete(long offset, long limit, Orphan<? extends Dedup<B, ?>> dedup) {
                super(offset, limit, dedup);
            }
            @Override public SlicingDedup<B> takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public long upstreamRequestLimit() {
            return Modifier.upstreamRequestLimit(skip, allowed);
        }

        @Override public String toString() {
            var sb = new StringBuilder();
            if (limit != Long.MAX_VALUE) sb.append("LIMIT " ).append(limit);
            if (offset >              0) sb.append("OFFSET ").append(offset);
            return sb.append(' ').append(dedup).toString();
        }

        @Override public boolean targetsProjection() {return true;}
        @Override public void rebind(BatchBinding binding) throws RebindException {
            skip = offset;
            allowed = limit;
            dedup.rebind(binding);
        }

        @Override public Decision drop(B batch, int row) {
            if (allowed == 0) return Decision.TERMINATE;
            if (dedup.isDuplicate(batch, row, 0)) return Decision.DROP;
            if (skip > 0) {
                --skip;
                return Decision.DROP;
            }
            --allowed;
            return Decision.KEEP;
        }
    }

    private static abstract sealed class SlicingFiltering<B extends Batch<B>>
            extends Filtering<B, SlicingFiltering<B>> {
        private final long offset, limit;
        private long skip, allowed;

        public SlicingFiltering(long offset, long limit, BatchType<B> bt, Vars inVars,
                                List<Expr> filters) {
            super(bt, inVars, filters);
            skip = this.offset = offset;
            allowed = this.limit = limit;
        }

        private static final class Concrete<B extends Batch<B>>
                extends SlicingFiltering<B>
                implements Orphan<SlicingFiltering<B>> {
            public Concrete(long offset, long limit, BatchType<B> bt, Vars inVars, List<Expr> filters) {
                super(offset, limit, bt, inVars, filters);
            }
            @Override public SlicingFiltering<B> takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public long upstreamRequestLimit() {
            return Modifier.upstreamRequestLimit(skip, allowed);
        }

        @Override public String toString() {
            var sb = new StringBuilder().append(super.toString());
            if (offset != 0)
                sb.append("OFFSET ").append(offset).append(' ');
            if (limit != Long.MAX_VALUE)
                sb.append("LIMIT ").append(limit).append(' ');
            if (sb.charAt(sb.length()-1) == ' ')
                sb.setLength(sb.length()-1);
            return sb.toString();
        }

        @Override public void rebind(BatchBinding binding) {
            super.rebind(binding);
            skip = offset;
            allowed = limit;
        }

        @Override public boolean isNoOp() {
            return skip <= 0  && allowed == Long.MAX_VALUE && super.isNoOp();
        }

        @Override public Decision drop(B batch, int row) {
            if (allowed == 0) return Decision.TERMINATE;
            var decision = super.drop(batch, row);
            if (decision != Decision.KEEP) return decision;
            if (skip > 0) {
                --skip;
                return Decision.DROP;
            }
            if (allowed != Long.MAX_VALUE)
                --allowed;
            return Decision.KEEP;
        }
    }

    public static abstract sealed class Filtering<B extends Batch<B>, R extends Filtering<B, R>>
            extends AbstractOwned<R>
            implements RowFilter<B, R> {
        private static final ExprEvaluator[] EMPTY_EVALUATORS = new ExprEvaluator[0];

        private final BatchBinding tmpBinding;
        private final Vars inVars;
        private List<Expr> filters;
        private ExprEvaluator[] evaluators;
        private final Vars.Mutable filterVars;
        private int failures = 0;

        public Filtering(BatchType<B> bt, Vars inVars, List<Expr> filters) {
            this.inVars     = inVars;
            this.tmpBinding = new BatchBinding(inVars);
            this.filterVars = new Vars.Mutable(10);
            setFilters(filters);
        }

        @Override public @Nullable R recycle(Object currentOwner) {
            internalMarkGarbage(currentOwner);
            for (ExprEvaluator e : evaluators)
                e.close();
            evaluators = EMPTY_EVALUATORS;
            return null;
        }

        private static final class Concrete<B extends Batch<B>, R extends Filtering<B, R>>
                extends Filtering<B, R>
                implements Orphan<R> {
            public Concrete(BatchType<B> bt, Vars inVars, List<Expr> filters) {
                super(bt, inVars, filters);
            }
            @Override public R takeOwnership(Object o) {return takeOwnership0(o);}
        }

        private void logFailure(Throwable t) {
            if (failures > 2) return;
            String stop = ++failures == 2 ? "Will stop reporting for this BIt" : "";
            log.info("Filter evaluation failed for {}. filters={}", tmpBinding, filters, t);
        }

        @Override public String toString() {
            try (var sb = PooledMutableRope.get()) {
                sb.append("FILTER(");
                if (!filters.isEmpty()) {
                    for (Expr e : filters) {
                        e.toSparql(sb, PrefixAssigner.CANON);
                        sb.append(" && ");
                    }
                    sb.unAppend(4);
                }
                return sb.append(')').toString();
            }
        }

        public void setFilters(List<Expr> filters) {
            this.filters = filters;
            filterVars.clear();
            int filtersCount = filters.size();
            if (filtersCount == 0)
                evaluators = EMPTY_EVALUATORS;
            else if (evaluators == null || evaluators.length != filtersCount)
                evaluators = new ExprEvaluator[filtersCount];
            for (int i = 0; i < evaluators.length; i++) {
                Expr expr = filters.get(i);
                evaluators[i] = expr.evaluator(inVars);
                filterVars.addAll(expr.vars());
            }
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            int n = evaluators.length;
            for (int i = 0; i < n; i++) {
                Expr e = filters.get(i), bound;
                if (e.vars().intersects(binding.vars) && (bound = e.bound(binding)) != e)
                    evaluators[i] = bound.evaluator(tmpBinding.vars);
            }
        }

        @Override public Vars bindableVars() { return filterVars; }

        @Override public boolean isNoOp() { return evaluators.length == 0; }

        @Override public Decision drop(B batch, int row) {
            var binding = this.tmpBinding.attach(batch, row);
            try {
                for (ExprEvaluator e : evaluators) {
                    if (!e.evaluate(batch, row).asBool()) return Decision.DROP;
                }
                return Decision.KEEP;
            } catch (Throwable t) {
                logFailure(t);
                return Decision.DROP;
            }
        }
    }
}
