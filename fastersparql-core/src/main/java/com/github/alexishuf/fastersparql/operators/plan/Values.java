package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.SingletonBIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Emitters;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.owned.LeakyOwner;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.sparql.DistinctType.WEAK;

public final class Values extends Plan implements LeakyOwner {
    private @Nullable TermBatch values;
    private @MonotonicNonNull TermBatch dedupValues;

    public Values(Vars vars, @Nullable Orphan<TermBatch> values) {
        super(Operator.VALUES);
        this.publicVars = this.allVars = vars;
        if (Batch.peekRows(values) == 0)
            Orphan.recycle(values);
        else if (values != null)
            this.values = values.takeOwnership(this);
    }

    @Override public Plan copy(@Nullable Plan[] ops) {
        return new Values(publicVars, values == null ? null : values.dup());
    }

    public TermBatch values() { return values; }

    public Orphan<TermBatch> takeValues() {
        if (values == null)
            return null;
        var orphan  = values.releaseOwnership(this);
        values      = null;
        dedupValues = Batch.safeRecycle(dedupValues, this);
        return orphan;
    }

    public void append(TermBatch other) {
        other.requireAlive();
        if (values == null)
            values = TERM.create(other.cols).takeOwnership(this);
        values.copy(other);
    }

    @Override public <B extends Batch<B>> BIt<B>
    execute(BatchType<B> batchType, @Nullable Binding binding, boolean weakDedup) {
        TermBatch values = this.values;
        if (weakDedup && values != null && values.rows > 1)
            values = dedupValues();
        return new ValuesBIt<>(batchType, values == null ? null
                               : batchType.convertOrCopy(values));
    }

    @Override
    public <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    doEmit(BatchType<B> type, Vars rebindHint, boolean weakDedup) {
        TermBatch values = this.values;
        if (weakDedup && values != null && values.rows > 1)
            values = dedupValues();
        if (values == null)
            return Emitters.empty(type, publicVars);
        return Emitters.ofBatch(publicVars, type.convertOrCopy(values));
    }

    private TermBatch dedupValues() {
        TermBatch dedupValues = this.dedupValues;
        if (dedupValues == null && values != null) {
            var filter = TERM.filter(publicVars, Dedup.weak(TERM, values.cols, WEAK))
                    .takeOwnership(this);
            this.dedupValues = dedupValues = filter.filterInPlace(values.dup()).takeOwnership(this);
            filter.recycle(this);
        }
        return dedupValues;
    }

    @Override public boolean equals(Object obj) {
        return super.equals(obj) && obj instanceof Values r && publicVars.equals(r.publicVars)
                                 && Objects.equals(values, r.values);
    }

    @Override public int hashCode() {
        return Objects.hash(type, publicVars, values);
    }

    @Override public String toString() {
        try (var sb = PooledMutableRope.get()) {
            algebraName(sb);
            sb.append('(');
            if (values == null)
                return sb.append(')').toString();
            int displayed = Math.min(10, values.rows);
            for (int r = 0; r < displayed; r++) {
                sb.append("\n  ");
                sb.append('[');
                for (int c = 0, n = publicVars.size(); c < n; c++) {
                    var term = values.get(r, c);
                    if (c > 0) sb.append(", ");
                    if (term == null)
                        sb.append("UNDEF");
                    else
                        term.toSparql(sb, PrefixAssigner.CANON);
                }
                sb.append("],");
            }
            if (values.rows > displayed || values.next != null)
                sb.append("\n  ...");
            else if (displayed > 0)
                sb.unAppend(1);
            return sb.append("\n)").toString();
        }
    }

    private static final byte[] VALUES_u8 = "VALUES".getBytes(StandardCharsets.UTF_8);
    private static final byte[] UNDEF_u8 = "UNDEF".getBytes(StandardCharsets.UTF_8);

    @Override public void groupGraphPatternInner(ByteSink<?, ?> out, int indent,
                                                 PrefixAssigner assigner,
                                                 Var2BNodeAssigner var2BNode) {
        out.newline(indent++).append(VALUES_u8).append(' ').append('(');
        for (int i = 0, n = publicVars.size(); i < n; i++) {
            if (i > 0) out.append(' ');
            out.append('?').append(publicVars.get(i));
        }
        out.append(')').append(' ').append('{');
        for (var b = values; b != null; b = b.next) {
            for (short r = 0, rows = b.rows, cols = b.cols; r < rows; r++) {
                out.newline(indent).append('(').append(' ');
                for (int c = 0; c < cols; c++) {
                    Term term = b.get(r, c);
                    if (term == null) out.append(UNDEF_u8);
                    else              term.toSparql(out, assigner);
                    out.append(' ');
                }
                out.append(')');
            }
        }
        out.newline(--indent).append('}');
    }

    private final class ValuesBIt<B extends Batch<B>> extends SingletonBIt<B> {
        ValuesBIt(BatchType<B> batchType, Orphan<B> values) {
            super(values, batchType, publicVars);
            metrics = Metrics.createIf(Values.this);
        }
        @Override public String toString() { return Values.this.toString(); }
    }
}
