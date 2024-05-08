package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.Label.MINIMAL;
import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.Label.SIMPLE;

public abstract class BatchMerger<B extends Batch<B>, P extends BatchMerger<B, P>>
        extends BatchProcessor<B, P> {
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
        return label(SIMPLE)+'('+(upstream == null ? null : upstream.label(MINIMAL))+')';
    }

    @Override public String label(StreamNodeDOT.Label type) {
        final StringBuilder sb = new StringBuilder();
        String id = Integer.toHexString(System.identityHashCode(this));
        if (columns != null) {
            sb.append("Project").append(vars).append('@').append(id);
        } else {
            sb.append("Merge([");
            for (int i = 0, n = vars.size(); i < n; i++) {
                if (sources[i] > 0) sb.append(vars.get(i)).append(", ");
            }
            if (sb.charAt(sb.length()-1) == ' ') sb.setLength(sb.length()-2);
            sb.append("], [");
            for (int i = 0, n = vars.size(); i < n; i++) {
                if (sources[i] < 0) sb.append(vars.get(i)).append(", ");
            }
            if (sb.charAt(sb.length()-1) == ' ') sb.setLength(sb.length()-2);
            sb.append("])@").append(id);
        }
        if (type.showState()) {
            sb.append("\nstate=").append(flags.render(state()))
                    .append(", upstreamCancelled=").append(upstreamCancelled());
        }
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    public abstract Orphan<B> projectInPlace(Orphan<B> batch);

    /** Implements {@link #projectInPlace(Orphan)} for {@code null} or empty inputs*/
    protected final Orphan<B> projectInPlaceEmpty(Orphan<B> orphan) {
        B b = Orphan.takeOwnership(orphan, this);
        for (B node = b; node != null; node = node.next)
            node.cols = (short)sources.length;
        return Owned.releaseOwnership(b, this);
    }

    /**
     * Performs a {@link #merge(Orphan, Batch, int, Batch)} or {@link #project(Orphan, Batch)}
     * operation when {@code this.sources.lenght == 0}.
     *
     * <p>If {@code dst == in}, {@link Batch#clear(int)} will be called to turn {@code dst}
     * into a zero-column batch, before anything. Next, in any case {@code dst} will receive
     * {@code in.rows} zero-column rows.</p>
     *
     * @param dst the batch that will receive the zero-with rows
     * @param in the {@code right} parameter from {@link #merge(Orphan, Batch, int, Batch)} or the
     *           {@code in} parameter from {@link #project(Orphan, Batch)}.
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
    public abstract Orphan<B> project(Orphan<B> dest, B in);

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
    public abstract Orphan<B> projectRow(@Nullable Orphan<B> dst, B in, int row);

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
    public abstract Orphan<B> merge(@Nullable Orphan<B> dest, B left, int leftRow, @Nullable B right);

    /**
     * Equivalent to {@link #merge(Orphan, Batch, int, Batch)} where right would be a
     * batch containing only the {@code rightRow}-th row of the {@code right} given in this call.
     */
    public abstract Orphan<B> mergeRow(@Nullable Orphan<B> dest, B left, int leftRow, B right, int rightRow);
}
