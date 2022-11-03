package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.client.model.row.dedup.Dedup;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

public final class DedupConcatBIt<R> extends MeteredConcatBIt<R> {
    private final Dedup<R> dedup;
    private @Nullable R polled = null;

    public DedupConcatBIt(List<? extends BIt<R>> sources, Plan<R, ?> plan,
                          Dedup<R> dedup) {
        super(sources, plan);
        this.dedup = dedup;
    }

    /* --- --- --- customize ConcatBIt behavior --- --- --- */

    @Override public boolean hasNext() {
        try {
            while (polled == null && super.hasNext()) {
                R row = source.next();
                if (!dedup.isDuplicate(row, sourceIdx))
                    polled = row;
            }
            return polled != null;
        } catch (Throwable t) { error = error == null ? t : error; throw t; }
    }

    @Override public R next() {
        try {
            if (!hasNext()) throw new NoSuchElementException();
            R r = projector == null ? polled : projector.merge(polled, null);
            polled = null;
            ++rows;
            return r;
        } catch (Throwable t) { error = error == null ? t : error; throw t; }
    }

    @Override public Batch<R> nextBatch() {
        try {
            while (true) {
                Batch<R> b = super.nextBatch();
                if (polled != null)  //cold branch
                    addPolled(b);
                if (b.size == 0)
                    return b;
                dedup.filter(b, sourceIdx, projector);
                rows += b.size;
                if (b.size > 0)
                    return b;
            }
        } catch (Throwable t) { error = error == null ? t : error; throw t; }
    }

    private void addPolled(Batch<R> b) {
        // grow b.array to fit polled
        if (b.array.length == b.size)
            b.array = Arrays.copyOf(b.array, b.array.length+1);

        // move polled to b.array[0]
        System.arraycopy(b.array, 1, b.array, 0, b.size++);
        b.array[0] = polled;
        polled = null;
    }
}
