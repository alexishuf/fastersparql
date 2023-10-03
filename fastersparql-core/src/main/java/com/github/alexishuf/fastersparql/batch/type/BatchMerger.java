package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class BatchMerger<B extends Batch<B>> extends BatchProcessor<B> {
    protected final int[] sources;
    public final int @Nullable [] columns;

    public static int[] mergerSources(Vars out, Vars leftVars, Vars rightVars) {
        int[] sources = new int[out.size()];
        for (int i = 0; i < sources.length; i++) {
            var var = out.get(i);
            int s = leftVars.indexOf(var);
            sources[i] = s >= 0 ? s+1 : -rightVars.indexOf(var)-1;
        }
        return sources;
    }

    public static int @Nullable [] projectorSources(Vars out, Vars leftVars) {
        if (out.equals(leftVars)) return null;
        return mergerSources(out, leftVars, Vars.EMPTY);
    }

    public BatchMerger(BatchType<B> batchType, Vars outVars, int[] sources) {
        super(batchType, outVars, CREATED, PROC_FLAGS);
        this.sources = sources;
        this.columns = makeColumns(sources);
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, outVars);
    }

    @Override public String toString() {
        String inner = label(StreamNodeDOT.Label.SIMPLE);
        if (upstream != null)
            return "<-"+inner+"- "+upstream;
        return (columns == null ? "Merge(" : "Project") + inner + (columns == null ? ")" : "");
    }

    @Override public String label(StreamNodeDOT.Label type) {
        final StringBuilder sb = new StringBuilder();
        if (columns != null) {
            sb.append(vars).append('@').append(System.identityHashCode(this));
        } else {
            sb.append('[');
            for (int i = 0, n = vars.size(); i < n; i++) {
                if (sources[i] > 0) sb.append(vars.get(i)).append(", ");
            }
            if (sb.charAt(sb.length()-1) == ' ') sb.setLength(sb.length()-2);
            sb.append("], [");
            for (int i = 0, n = vars.size(); i < n; i++) {
                if (sources[i] < 0) sb.append(vars.get(i)).append(", ");
            }
            if (sb.charAt(sb.length()-1) == ' ') sb.setLength(sb.length()-2);
            sb.append("]");
        }
        if (type.showState()) {
            sb.append("\nstate=").append(flags.render(state()))
                    .append(", upstreamCancelled=").append(upstreamCancelled());
        }
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    protected int[] makeColumns(int[] sources) {
        boolean isProjection = true;
        for (int s : sources) {
            if (s < 0) { isProjection = false; break; }
        }
        if (!isProjection)
            return null;
        int[] columns = new int[sources.length];
        for (int i = 0; i < sources.length; i++)
            columns[i] = sources[i]-1;
        return columns;
    }

    @Override public final B processInPlace(B b) { return projectInPlace(b); }

    public abstract B projectInPlace(B batch);

    /**
     * Projects {@code in} (removing/adding columns) and writes resulting rows to {@code dest}.
     *
     * @param dest where projected rows wil be written
     * @param in   batch with rows to project
     * @return batch that received the projected rows. {@code dest} (if non-null), a
     * recycled batch or a newly allocated batch.
     */
    public B project(B dest, B in) {
        if (columns == null) throw new UnsupportedOperationException();
        int rows = in.rows;
        if (dest == null)
            dest = getBatch(rows, columns.length, in.localBytesUsed());
        for (int r = 0; r < rows; r++) {
            dest.beginPut();
            for (int c = 0; c < columns.length; c++) {
                int src = columns[c];
                if (src >= 0) dest.putTerm(c, in, r, src);
            }
            dest.commitPut();
        }
        return dest;
    }

    /**
     * Appends a projection of the {@code row}-th row in {@code in} to {@code dst}.
     *
     * @param dst the destination batch that will receive a new row. If {@code null}, a
     *            new batch will be created via {@link #batchType()}. If non-null, it
     *            will not be {@link Batch#clear()}ed and must have
     *            {@link Batch#cols}{@code ==}{@link #vars()}{@code .size()}.
     * @param in the source batch containing a row to be projected
     * @param row the row in {@code in} to be projected.
     */
    public B projectRow(@Nullable B dst, B in, int row) {
        if (columns == null) throw new UnsupportedOperationException();
        if (dst == null)
            dst = getBatch(1, columns.length, in.localBytesUsed(row));
        else if (dst.cols != columns.length)
            throw new IllegalArgumentException("dst.cols != columns.length");
        dst.beginPut();
        for (int c = 0; c < columns.length; c++) {
            int src = columns[c];
            if (src >= 0) dst.putTerm(c, in, row, src);
        }
        dst.commitPut();
        return dst;
    }

    /**
     * Add {@code right.rows} to {@code dest} (or to a new {@link Batch} if {@code null})
     * where column {@code c} of row {@code r} is sourced from either a column in
     * {@code left}'s {@code leftRow} or from a column in the {@code r}-th row of {@code right}.
     *
     * @param dest    if not null, merged rows will be appended to this batch (it will not
     *                be {@link Batch#clear()}ed).
     * @param left    batch containing the left row
     * @param leftRow index of the left row in {@code left}
     * @param right   batch of rows to merge with {@code left}'s {@code leftRow}
     * @return {@code dest}, if not null, else a new {@link Batch}.
     */
    public B merge(@Nullable B dest, B left, int leftRow, @Nullable B right) {
        if (right == null || right.rows == 0)
            return mergeWithMissing(dest, left, leftRow);
        int rows = right.rows;
        if (dest == null) {
            int local = right.localBytesUsed() + rows * left.localBytesUsed(leftRow);
            dest = getBatch(rows, sources.length, local);
        } else if (dest.cols != sources.length) {
            throw new IllegalArgumentException("dest.cols != sources.length");
        }
        for (int i = 0; i < rows; i++) {
            dest.beginPut();
            for (int c = 0; c < sources.length; c++) {
                int s = sources[c];
                if      (s > 0) dest.putTerm(c, left, leftRow, s - 1);
                else if (s < 0) dest.putTerm(c, right, i, -s - 1);
            }
            dest.commitPut();
        }
        return dest;
    }

    /**
     * Performs a {@link BatchMerger#merge(Batch, Batch, int, Batch)} with null/empty right batch.
     *
     * @param dest if non-null, lefRow of {@code left} will be appended (see {@link Batch#beginPut()}
     * @param left batch with a left row
     * @param leftRow index of the left row in {@code left} batch
     * @return a batch (which may be {@code dest} containing the left row in its end, with null
     *         on columns that would be assigned from a right row
     */
    public B mergeWithMissing(@Nullable B dest, B left, int leftRow) {
        if (dest == null)
            dest = getBatch(1, sources.length, left.localBytesUsed(leftRow));
        dest.beginPut();
        for (int c = 0; c < sources.length; c++) {
            int s = sources[c];
            if   (s > 0) dest.putTerm(c, left, leftRow, s-1);
        }
        dest.commitPut();
        return dest;
    }

}
