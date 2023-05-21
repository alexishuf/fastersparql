package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.SingletonBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;

public final class Values extends Plan {
    private final @Nullable TermBatch values;
    private @MonotonicNonNull TermBatch dedupValues;

    public Values(Vars vars, @Nullable TermBatch values) {
        super(Operator.VALUES);
        this.publicVars = this.allVars = vars;
        this.values = values != null && values.rows == 0 ? null : values;
    }

    @Override public Plan copy(@Nullable Plan[] ops) { return new Values(publicVars, values); }

    public TermBatch values() { return values; }

    @Override public <B extends Batch<B>> BIt<B>
    execute(BatchType<B> batchType, @Nullable Binding binding, boolean canDedup) {
        TermBatch values = this.values;
        if (canDedup && values != null && values.rows > 1)
            values = dedupValues();
        return new ValuesBIt<>(batchType, values == null ? null : batchType.convert(values));
    }

    private TermBatch dedupValues() {
        TermBatch dedupValues = this.dedupValues;
        if (dedupValues == null && values != null) {
            var dedup = TERM.dedupPool.getWeak(values.rows, values.cols);
            var filter = TERM.filter(dedup);
            this.dedupValues = dedupValues = filter.filter(null, values);
            filter.release();
            TERM.dedupPool.offerWeak(dedup);
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
        var sb = new ByteRope();
        sb.append(algebraName()).append('(');
        if (values == null)
            return sb.append(')').toString();
        int displayed = Math.min(10, values.rows);
        for (int r = 0; r < displayed; r++) {
            sb.append("\n  ");
            sb.append('[');
            for (int c = 0, n = publicVars.size(); c < n; c++) {
                var term = values.get(r, c);
                (c > 0 ? sb.append(", ") : sb).append(term == null ? "UNDEF" : term.toSparql());
            }
            sb.append("],");
        }
        if (values.rows > displayed)
            sb.append("\n  ...");
        else if (displayed > 0)
            sb.unAppend(1);
        return sb.append("\n)").toString();
    }

    private static final byte[] VALUES_u8 = "VALUES".getBytes(StandardCharsets.UTF_8);
    private static final byte[] UNDEF_u8 = "UNDEF".getBytes(StandardCharsets.UTF_8);

    @Override public void groupGraphPatternInner(ByteSink<?> out, int indent, PrefixAssigner assigner) {
        out.newline(indent++).append(VALUES_u8).append(' ').append('(');
        for (int i = 0, n = publicVars.size(); i < n; i++) {
            if (i > 0) out.append(' ');
            out.append('?').append(publicVars.get(i));
        }
        out.append(')').append(' ').append('{');
        if (values != null) {
            for (int r = 0, rows = values.rows, cols = values.cols; r < rows; r++) {
                out.newline(indent).append('(').append(' ');
                for (int c = 0; c < cols; c++) {
                    Term term = values.get(r, c);
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
        ValuesBIt(BatchType<B> batchType, B values) {
            super(values, batchType, publicVars);
            metrics = Metrics.createIf(Values.this);
        }

        @Override protected void cleanup(@Nullable Throwable error) {
            super.cleanup(error);
        }

        @Override public String toString() { return Values.this.toString(); }
    }
}
