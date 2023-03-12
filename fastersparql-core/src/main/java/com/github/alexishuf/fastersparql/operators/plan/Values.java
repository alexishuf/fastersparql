package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItClosedAtException;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.model.row.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class Values extends Plan {
    private final List<Term[]> rows;

    public Values(Vars vars, List<Term[]> rows) {
        super(Operator.VALUES);
        this.publicVars = this.allVars = vars;
        this.rows = rows;
    }

    @Override public Plan copy(@Nullable Plan[] ops) { return new Values(publicVars, rows); }

    public List<Term[]> rows() { return rows; }

    @Override public <T> BIt<T> execute(RowType<T> rowType, @Nullable Binding binding,
                                        boolean canDedup) {
        List<T> source;
        var converter = rowType.converter(RowType.ARRAY, publicVars);
        if (canDedup && rows.size() > 1) {
            source = new ArrayList<>(rows.size());
            var dedup = StrongDedup.strongUntil(RowType.ARRAY, rows.size());
            for (Term[] r : rows) {
                if (!dedup.isDuplicate(r, 0))
                    source.add(converter.apply(r));
            }
        } else if (rowType.equals(RowType.ARRAY)) {//noinspection unchecked
            source = (List<T>) this.rows;
        } else {
            source = new ArrayList<>(rows.size());
            for (Term[] row : rows)
                source.add(converter.apply(row));
        }
        return new ValuesBit<>(rowType, source);
    }

    @Override public boolean equals(Object obj) {
        return super.equals(obj) && obj instanceof Values r && publicVars.equals(r.publicVars)
                                 && rows.equals(r.rows);
    }

    @Override public int hashCode() {
        return Objects.hash(type, publicVars, rows);
    }

    @Override public String toString() {
        var sb = new StringBuilder();
        sb.append(algebraName()).append('(');
        int displayed = Math.min(10, rows.size());
        for (int i = 0; i < displayed; i++) {
            sb.append("\n  ");
            var r = rows.get(i);
            sb.append('[');
            for (int col = 0, n = publicVars.size(); col < n; col++) {
                var term = r[col];
                (col > 0 ? sb.append(", ") : sb).append(term == null ? "UNDEF" : term.toSparql());
            }
            sb.append("],");
        }
        if (rows.size() > displayed)
            sb.append("\n  ...");
        else if (displayed > 0)
            sb.setLength(sb.length()-1);
        return sb.append("\n)").toString();
    }

    private static final byte[] VALUES_u8 = "VALUES".getBytes(StandardCharsets.UTF_8);
    private static final byte[] UNDEF_u8 = "UNDEF".getBytes(StandardCharsets.UTF_8);

    @Override public void groupGraphPatternInner(ByteRope out, int indent, PrefixAssigner assigner) {
        out.newline(indent++).append(VALUES_u8).append(' ').append('(');
        for (Rope name : publicVars)
            out.append('?').append(name).append(' ');
        if (!publicVars.isEmpty())
            out.unAppend(1);
        out.append(')').append(' ').append('{');
        for (Term[] row : rows) {
            out.newline(indent).append('(').append(' ');
            for (Term term : row) {
                if (term == null) out.append(UNDEF_u8);
                else              term.toSparql(out, assigner);
                out.append(' ');
            }
            out.append(')');
        }
        out.newline(--indent).append('}');
    }

    private final class ValuesBit<T> extends IteratorBIt<T> {
        private final @Nullable Metrics metrics;

        public ValuesBit(RowType<T> rowType, List<T> rows) {
            super(rows.iterator(), rowType, Values.this.publicVars);
            metrics = Metrics.createIf(Values.this);
        }

        @Override protected void cleanup(@Nullable Throwable error) {
            super.cleanup(error);
            if (metrics != null)
                metrics.complete(error, BItClosedAtException.isClosedFor(error, this)).deliver();
        }

        @Override public T next() {
            var next = super.next();
            if (metrics != null) metrics.rowsEmitted(1);
            return next;
        }

        @Override public Batch<T> nextBatch() {
            var b = super.nextBatch();
            if (metrics != null) metrics.rowsEmitted(b.size);
            return b;
        }

        @Override public String toString() { return Values.this.toString(); }
    }
}
