package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.batch.operators.ProcessorBIt;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.ExprEvaluator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.FSProperties.reducedCapacity;
import static com.github.alexishuf.fastersparql.batch.dedup.StrongDedup.strongUntil;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.*;

@SuppressWarnings("unused")
public final class Modifier extends Plan {
    private static final Logger log = LoggerFactory.getLogger(Modifier.class);

    public @Nullable Vars projection;
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
        var sb = new ByteRope();
        sb.append(algebraName()).append(left);

        if (!filters.isEmpty())                  sb.append(')');
        if (distinctCapacity > 0)                sb.append(')');
        if (limit > 0 && limit < Long.MAX_VALUE) sb.append(')');
        if (offset > 0)                          sb.append(')');
        if (projection != null)                  sb.append(')');
        return sb.toString();
    }

    @Override public SegmentRope sparql() {
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
                (weakDedup && offset <= 0) || distinctCapacity > 0);
        return executeFor(in, binding, weakDedup && distinctCapacity == 0);
    }

    @Override
    public <B extends Batch<B>> Emitter<B> doEmit(BatchType<B> type, Vars rebindHint,
                                                  boolean weakDedup) {
        return processed(left().emit(type, rebindHint, weakDedup), weakDedup);
    }

    public <B extends Batch<B>>
    BIt<B> executeFor(BIt<B> in, @Nullable Binding binding, boolean weakDedup) {
        var processor = processorFor(in.batchType(), in.vars(), binding, weakDedup);
        if (processor == null) return in;
        return new ProcessorBIt<>(in, processor, Metrics.createIf(this));
    }

    public <B extends Batch<B>> Emitter<B> processed(Emitter<B> in) {
        return processed(in, false);
    }

    public <B extends Batch<B>> Emitter<B>
    processed(Emitter<B> in, boolean weakDedup) {
        var proc = processorFor(in.batchType(), in.vars(), null, weakDedup);
        if (proc == null)
            return in;
        proc.subscribeTo(in);
        return proc;
    }

    public <B extends Batch<B>>
    BatchProcessor<B> processorFor(BatchType<B> bt, Vars inVars,
                                   @Nullable Binding binding, boolean weakDedup) {
        Vars outVars = projection == null ? inVars : projection;
        if (binding != null)
            outVars = outVars.minus(binding.vars());
        List<Expr> filters = binding == null ? this.filters : boundFilters(binding);
        int dCap = distinctCapacity, cols = outVars.size();
        long limit = this.limit;

        Dedup<B> dedup = null;
        if      (cols == 0)                               limit = dCap > 0 || weakDedup ? 1 : limit;
        else if (dCap >= reducedCapacity() && !weakDedup) dedup = strongUntil(bt, dCap, cols);
        else if (dCap > 0)                                dedup = new WeakDedup<>(bt, dCap, cols);

        BatchProcessor<B> processor;
        boolean slice = limit < Long.MAX_VALUE || offset > 0;
        if (!filters.isEmpty()) {
            var rf = slice && dedup == null ? new SlicingFiltering<>(offset, limit, bt, inVars, filters)
                                            : new Filtering<>(bt, inVars, filters);
            processor = bt.filter(outVars, inVars, rf);
            if (dedup != null) {
                RowFilter<B> dedupRF = slice ? new SlicingDedup<>(offset, limit, dedup) : dedup;
                processor = bt.filter(outVars, dedupRF, (BatchFilter<B>) processor);
            }
        } else if (dedup == null) {
            processor = slice ? bt.filter(outVars, inVars, new Slicing<>(offset, limit))
                              : bt.projector(outVars, inVars);
        } else {
            var rf = slice ? new SlicingDedup<>(offset, limit, dedup) : dedup;
            processor = bt.filter(outVars, inVars, rf);
        }
        return processor;
    }


    /* --- --- --- RowFilter implementations --- --- --- */

    private static long upstreamRequestLimit(long offset, long limit) {
        long sum = offset + limit;
        return sum < 0 ? Long.MAX_VALUE : sum;
    }

    private static class Slicing<B extends Batch<B>> implements RowFilter<B> {
        private final long offset, limit;
        private long skip, allowed;

        public Slicing(long offset, long limit) {
            skip = this.offset = offset;
            allowed = this.limit = limit;
        }
        @Override public long upstreamRequestLimit() {
            return Modifier.upstreamRequestLimit(skip, allowed);
        }
        @Override public void rebind(BatchBinding binding) throws RebindException {
            skip = offset;
            allowed = limit;
        }

        @Override public Decision drop(B batch, int row) {
            if (skip > 0) {
                --skip;
                return Decision.DROP;
            } else if (allowed == 0) {
                return Decision.TERMINATE;
            }
            --allowed;
            return Decision.KEEP;
        }
    }

    private static class SlicingDedup<B extends Batch<B>> implements RowFilter<B> {
        private final Dedup<B> dedup;
        private final long offset, limit;
        private long skip, allowed;

        public SlicingDedup(long offset, long limit, Dedup<B> dedup) {
            this.dedup = dedup;
            skip = this.offset = offset;
            allowed = this.limit = limit;
        }

        @Override public void rebindAcquire() {
            dedup.rebindAcquire();
        }

        @Override public void rebindRelease() {
            dedup.rebindRelease();
        }

        @Override public void release() {
            dedup.release();
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

    private static final class SlicingFiltering<B extends Batch<B>> extends Filtering<B> {
        private final long offset, limit;
        private long skip, allowed;

        public SlicingFiltering(long offset, long limit, BatchType<B> bt, Vars inVars,
                                List<Expr> filters) {
            super(bt, inVars, filters);
            skip = this.offset = offset;
            allowed = this.limit = limit;
        }
        @Override public void rebind(BatchBinding binding) {
            skip = offset;
            allowed = limit;
            super.rebind(binding);
        }

        @Override public Decision drop(B batch, int row) {
            if (allowed == 0) return Decision.TERMINATE;
            var decision = super.drop(batch, row);
            if (decision != Decision.KEEP) return decision;
            if (skip > 0) {
                --skip;
                return Decision.DROP;
            }
            --allowed;
            return Decision.KEEP;
        }
    }

    private static class Filtering<B extends Batch<B>> implements RowFilter<B> {
        private final BatchBinding tmpBinding;
        private final List<Expr> filters;
        private final ExprEvaluator[] evaluators;
        private int failures = 0;

        public Filtering(BatchType<B> bt, Vars inVars, List<Expr> filters) {
            this.tmpBinding = new BatchBinding(inVars);
            this.filters = filters;
            this.evaluators = new ExprEvaluator[filters.size()];
            for (int i = 0; i < evaluators.length; i++)
                evaluators[i] = filters.get(i).evaluator(inVars);
        }

        private void logFailure(Throwable t) {
            if (failures > 2) return;
            String stop = ++failures == 2 ? "Will stop reporting for this BIt" : "";
            log.info("Filter evaluation failed for {}. filters={}", tmpBinding, filters, t);
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            int n = evaluators.length;
            for (int i = 0; i < n; i++) {
                Expr e = filters.get(i), bound;
                if (e.vars().intersects(binding.vars) && (bound = e.bound(binding)) != e)
                    evaluators[i] = bound.evaluator(tmpBinding.vars);
            }
        }

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
