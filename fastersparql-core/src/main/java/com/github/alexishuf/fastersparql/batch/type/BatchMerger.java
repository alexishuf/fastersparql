package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class BatchMerger<B extends Batch<B>> extends BatchProcessor<B> {
    public final Vars outVars;
    protected final int[] sources;
    protected final int @Nullable [] columns;

    public static int @Nullable [] mergerSources(Vars out, Vars leftVars, Vars rightVars) {
        int[] sources = new int[out.size()];
        boolean trivial = out.size() == leftVars.size();
        for (int i = 0; i < sources.length; i++) {
            Rope var = out.get(i);
            int s = leftVars.indexOf(var);
            trivial &= s == i;
            sources[i] = s >= 0 ? s+1 : -rightVars.indexOf(var)-1;
        }
        return trivial ? null : sources;
    }

    public BatchMerger(BatchType<B> batchType, Vars outVars, int[] sources) {
        super(batchType);
        this.outVars = outVars;
        this.sources = sources;
        this.columns = makeColumns(sources);
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

    @Override public final B process(B b) { return project(null, b); }

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
            dest = getBatch(rows, sources.length, in.bytesUsed());
        for (int r = 0; r < rows; r++) {
            dest.beginPut();
            for (int c : columns) {
                if (c < 0) dest.putTerm(null);
                else       dest.putTerm(in, r, c);
            }
            dest.commitPut();
        }
        return dest;
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
        if (right == null)
            return mergeWithMissing(dest, left, leftRow);
        int rows = right.rows, bytesCapacity = right.bytesUsed() + left.bytesUsed(leftRow);
        if (dest == null)
            dest = getBatch(rows, sources.length, bytesCapacity);
        else if (dest.cols != sources.length)
            throw new IllegalArgumentException("dest.cols != sources.length");
        for (int i = 0; i < rows; i++) {
            dest.beginPut();
            for (int s : sources) {
                if (s > 0) dest.putTerm(left, leftRow, s - 1);
                else if (s < 0) dest.putTerm(right, i, -s - 1);
                else dest.putTerm(null);
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
            dest = getBatch(1, sources.length, left.bytesUsed(leftRow));
        dest.beginPut();
        for (int s : sources) {
            if   (s > 0) dest.putTerm(left, leftRow, s-1);
            else         dest.putTerm(null);
        }
        dest.commitPut();
        return dest;
    }

}