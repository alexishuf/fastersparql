package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.util.NoSuchElementException;

/** A {@link BIt} that simultaneously filters and transforms items from another {@link BIt} */
public abstract class FilteringTransformBIt<O, I> extends DelegatedControlBIt<O, I> {
    private int partialBegin = 0;
    private boolean clean, terminalProcessed;
    private final boolean sameType;
    private @Nullable Batch<O> partial, recycled;

    public FilteringTransformBIt(BIt<I> in, RowType<O> rowType, Vars vars) {
        super(in, rowType, vars);
        sameType = in.rowType().equals(rowType);
    }

    /**
     * Either take a previously {@link FilteringTransformBIt#recycle(Batch)}ed batch
     * or create a new {@link Batch}.
     *
     * @param minCapacity the minimum capacity required for the batch to be returned by this method.
     * @return An empty {@link Batch} with {@code b.array.length >= minCapacity}.
     */
    protected final Batch<O> recycleOrAlloc(int minCapacity) {
        if (recycled == null) {
            return new Batch<>(rowType.rowClass, minCapacity);
        } else {
            Batch<O> b = recycled;
            if (b.array.length < minCapacity) //noinspection unchecked
                b.array = (O[]) Array.newInstance(rowType.rowClass, minCapacity);
            b.size = 0;
            recycled = null;
            return b;
        }
    }

    /**
     * Filter and transform all items in {@code input}, in-place or generating a new {@link Batch}.
     *
     * <p>If the operation is not done in-place, this method implementation is responsible
     * for calling {@code delegate.recycle(input)}. In addition, new {@link Batch} instances
     * should be acquired via {@link FilteringTransformBIt#recycleOrAlloc(int)}, which reuses
     * batches offered to {@link FilteringTransformBIt#recycle(Batch)}</p>
     *
     * <p>This method will be called at most once for the empty {@link Batch} returned by
     * {@code delegate} that symbolizes the end of its iteration. Implementations may use
     * this opportunity to inject items rather than simply filtering and transforming.</p>
     *
     * @return {@code input} if the operation could be done in-place, else a new {@link Batch}.
     */
    protected abstract Batch<O> process(Batch<I> input);

    /**
     * This method will be called once after the first time {@code delegate.nextBatch()}
     * returns an empty {@link Batch} symbolizing the end of its iteration. The {@link Batch}
     * returned by this method wil be returned by {@link FilteringTransformBIt#nextBatch()}
     * instead of the empty {@link Batch}. The use case for this is injecting items after
     * iteration completes.
     *
     * @return The {@link Batch} that {@link FilteringTransformBIt#nextBatch()} will return
     *         the first time {@code delegate.nextBatch().size == 0}.
     */
    protected Batch<O> terminal() { return Batch.terminal(); }

    /**
     * This method will be called once per instance once an error is raised for the first
     * time during iteration or if iteration reaches the end (all items are delivered downstream).
     *
     * @param error the error that caused termination or {@code null} if this is being called due
     *              to complete iteration.
     */
    protected void cleanup(Throwable error) {
        delegate.close();
    }

    @Override public final Batch<O> nextBatch() {
        try {
            Batch<O> b = partial;
            if (partial != null) {
                return consumePartial(b);
            } else {
                while (true) {
                    Batch<I> in = delegate.nextBatch();
                    if (in.size == 0)
                        return handleTerminal();
                    b = process(in);
                    if (b.size > 0)
                        return b;
                    if (sameType) //noinspection unchecked
                        delegate.recycle((Batch<I>) b);
                }
            }
        } catch (Throwable t) {
            if (!clean) {
                clean = true;
                cleanup(t);
            }
            throw t;
        }
    }

    private Batch<O> handleTerminal() {
        if (terminalProcessed) {
            return Batch.terminal();
        } else {
            terminalProcessed = true;
            return terminal();
        }
    }

    private Batch<O> consumePartial(Batch<O> b) {
        if (partialBegin > 0) {
            assert partial != null;
            O[] a = partial.array;
            System.arraycopy(a, partialBegin, a, 0, partial.size-=partialBegin);
        }
        partial = null;
        return b;
    }

    @Override public final boolean hasNext() {
        while (partial == null) {
            partial = nextBatch();
            partialBegin = 0;
        }
        return partialBegin < partial.size;
    }

    @Override public final O next() {
        if (!hasNext()) throw new NoSuchElementException();
        assert partial != null;
        O item = partial.array[partialBegin++];
        if (partialBegin == partial.size)
            partial = null;
        return item;
    }

    @Override public final boolean recycle(Batch<O> batch) {
        //noinspection unchecked
        if (sameType && delegate.recycle((Batch<I>) batch)) return true;
        if (recycled == null || recycled.array.length < batch.array.length) {
            recycled = batch;
            return true;
        }
        return false;
    }
}
