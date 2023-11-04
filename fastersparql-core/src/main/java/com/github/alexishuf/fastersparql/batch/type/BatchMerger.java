package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class BatchMerger<B extends Batch<B>> extends BatchProcessor<B> {
    protected final short[] sources;
    public final short @Nullable [] columns;
    public final boolean safeInPlaceProject;

    public static short[] mergerSources(Vars out, Vars leftVars, Vars rightVars) {
        short[] sources = new short[out.size()];
        for (int i = 0; i < sources.length; i++) {
            var var = out.get(i);
            int s = leftVars.indexOf(var);
            sources[i] = (short)(s >= 0 ? s+1 : -rightVars.indexOf(var)-1);
        }
        return sources;
    }

    public static short @Nullable [] projectorSources(Vars out, Vars leftVars) {
        if (out.equals(leftVars)) return null;
        return mergerSources(out, leftVars, Vars.EMPTY);
    }

    public BatchMerger(BatchType<B> batchType, Vars outVars, short[] sources) {
        super(batchType, outVars, CREATED, PROC_FLAGS);
        this.sources = sources;

        boolean isProjection = true, safeInPlace = true;
        int leftCols = 0;
        for (int c = 0, src; c < sources.length; c++) {
            if ((src = sources[c]) < 0) {
                safeInPlace = isProjection = false;
                break;
            } else if (src > 0) {
                if (src   > leftCols) leftCols    = src;
                if (src-1 <        c) safeInPlace = false;
            }
        }
        this.safeInPlaceProject = safeInPlace && sources.length <= leftCols;
        if (isProjection) {
            columns = new short[sources.length];
            for (int i = 0; i < sources.length; i++)
                columns[i] = (short)(sources[i]-1);
        } else {
            columns = null;
        }
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

    public abstract B projectInPlace(B batch);

    /**
     * Performs a projection ({@link #project(Batch, Batch)} using {@code b} as both the input
     * and the destination (if possible). If {@code b} cannot be safely and efficiently used as
     * the destination, this method will return a new batch and {@code b} will be recycled before
     * this call returns.
     *
     * <p>Callers <strong>MUST</strong> cede ownership of {@code b} and take ownership of the batch
     * returned by this call (which MAY be {@code b}:</p>
     *
     * <pre>{@code
     *   b = merger.projectInPlace(b);
     * }</pre>
     *
     * @param b the input batch to be projected
     * @return a projection of {@code b}, which MAY be {@code b} itself.
     */
    protected final B projectInPlaceEmpty(B b) {
        if (b == null) return null;
        short rows = b.rows;
        b = b.clear(sources.length);
        b.rows = rows;
        return b;
    }

    /**
     * Performs a {@link #merge(Batch, Batch, int, Batch)} or {@link #project(Batch, Batch)}
     * operation when {@code this.sources.lenght == 0}.
     *
     * <p>If {@code dst == in}, {@link Batch#clear(int)} will be called to turn {@code dst}
     * into a zero-column batch, before anything. Next, in any case {@code dst} will receive
     * {@code in.rows} zero-column rows.</p>
     *
     * @param dst the batch that will receive the zero-with rows
     * @param in the {@code right} parameter from {@link #merge(Batch, Batch, int, Batch)} or the
     *           {@code in} parameter from {@link #project(Batch, Batch)}.
     * @return {@code dst}
     */
    protected final B mergeThin(B dst, @Nullable B in) {
        int rows = in == null ? 1 : in.totalRows();
        if (dst == in) dst.clear(0);
        dst.addRowsToZeroColumns(rows);
        return dst;
    }

    /**
     * Projects {@code in} (removing/adding columns) and writes resulting rows to {@code dest}.
     *
     * @param dest where projected rows wil be written
     * @param in   batch with rows to project
     * @return batch that received the projected rows. {@code dest} (if non-null), a
     * recycled batch or a newly allocated batch.
     */
    public abstract B project(B dest, B in);

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
    public abstract B projectRow(@Nullable B dst, B in, int row);

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
    public abstract B merge(@Nullable B dest, B left, int leftRow, @Nullable B right);
}
