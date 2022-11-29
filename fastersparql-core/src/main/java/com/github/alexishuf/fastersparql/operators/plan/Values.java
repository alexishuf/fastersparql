package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.model.row.dedup.StrongDedup;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import static com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics.buildAndSend;

public final class Values<R, I> extends Plan<R, I> {
    private final Vars vars;
    private final List<R> rows;

    public Values(RowType<R, I> rowType, Vars vars, List<R> rows,
                  @Nullable Plan<R, I> unbound, @Nullable String name) {
        super(rowType, List.of(), unbound, name);
        this.vars = vars;
        this.rows = rows;
        assert rows.stream().allMatch(r -> {
            if (r instanceof Object[] arr) return arr.length >= vars.size();
            else if (r instanceof Collection<?> coll) return coll.size() >= vars.size();
            return true;
        }) : "Some rows have less columns than #vars";
    }

    public List<R> rows() { return rows; }

    @Override protected Vars computeVars(boolean all) { return vars; }

    @Override public String algebraName() {
        return "Values"+vars;
    }


    @Override public BIt<R> execute(boolean canDedup) {
        return new ValuesBIt<>(this, canDedup);
    }

    @Override
    public Plan<R, I> with(List<? extends Plan<R, I>> replacement, @Nullable Plan<R, I> unbound,
                        @Nullable String name) {
        if (!replacement.isEmpty())
            throw new IllegalArgumentException("Expected no operands, got "+replacement.size());
        unbound = unbound == null ? this.unbound : unbound;
        name    = name    == null ? this.name    : name;
        return new Values<>(rowType, vars, rows, unbound, name);
    }

    @Override public String toString() {
        var sb = new StringBuilder();
        sb.append(algebraName()).append('(');
        int displayed = Math.min(10, rows.size());
        for (int i = 0; i < displayed; i++)
            sb.append("\n  ").append(rows.get(i)).append(',');
        if (rows.size() > displayed)
            sb.append(" ...");
        else if (displayed > 0)
            sb.setLength(sb.length()-1);
        return sb.append("\n)").toString();
    }

    @Override public void groupGraphPatternInner(StringBuilder out, int indent) {
        newline(out, indent++).append("VALUES ( ");
        for (String name : vars)
            out.append('?').append(name).append(' ');
        out.append(") {");
        for (R row : rows) {
            newline(out, indent).append("( ");
            for (int i = 0, n = vars.size(); i < n; i++) {
                String sparql = rowType.toSparql(rowType.get(row, i));
                out.append(sparql == null ? "UNDEF" : sparql).append(' ');
            }
            out.append(')');
        }
        newline(out, --indent).append('}');
    }

    private static final class ValuesBIt<R> extends AbstractBIt<R> {
        private final Values<R, ?> plan;
        private int pos = 0, rows = 0;
        private @Nullable Batch<R> recycled;
        private final @Nullable StrongDedup<R> dedup;
        private final long startNanos = System.nanoTime();

        public ValuesBIt(Values<R, ?> plan, boolean canDedup) {
            super(plan.rowType.rowClass, plan.vars);
            this.plan = plan;
            dedup = canDedup ? new StrongDedup<>(plan.rowType, (int) (plan.rows.size() / 0.75f + 1.0f))
                             : null;
        }

        @Override public @This BIt<R> tempEager() { return this; }

        @Override public boolean recycle(Batch<R> batch) {
            if (pos >= plan.rows.size() || recycled != null) return false;
            recycled = batch;
            return true;
        }

        @Override public Batch<R> nextBatch() {
            List<R> rows = plan.rows;
            int rowsSize = rows.size(), end = Math.min(rowsSize, pos+maxBatch), len = end - pos;
            Batch<R> batch;
            if (recycled == null) {
                batch = new Batch<>(elementClass, len);
            } else {
                (batch = recycled).size = 0;
                recycled = null;
            }
            if (len == 0) {
                onExhausted();
                return batch;
            } else if (dedup == null) {
                List<R> src = pos == 0 && end == rowsSize ? rows : rows.subList(pos, end);
                batch.array = src.toArray(batch.array);
                batch.size = len;
            } else {
                addDedup(rows, end, batch);
            }
            pos = end;
            this.rows += batch.size;
            return batch;
        }

        private void addDedup(List<R> rows, int end, Batch<R> batch) {
            assert dedup != null;
            for (int i = pos; i < end; i++) {
                R row = rows.get(i);
                if (!dedup.isDuplicate(row, 0))
                    batch.add(row);
            }
        }

        @Override public boolean hasNext() {
            return pos < plan.rows.size();
        }

        @Override public R next() {
            try {
                return plan.rows.get(pos++);
            } catch (IndexOutOfBoundsException e) { throw new NoSuchElementException(); }
        }

        @Override protected void cleanup(boolean interrupted) {
            buildAndSend(plan, rows, startNanos, null, interrupted);
        }

        @Override public String toString() {
            var sb = new StringBuilder().append(plan.vars).append("<-[");
            for (int i = 0, n = Math.min(3, plan.rows.size()); i < n; i++)
                sb.append(plan.rowType.toString(plan.rows.get(i))).append(", ");
            if (!plan.rows.isEmpty())
                sb.setLength(sb.length() - 2);
            return sb.append(']').toString();
        }
    }
}
