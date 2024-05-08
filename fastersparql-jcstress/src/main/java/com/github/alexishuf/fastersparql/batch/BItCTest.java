package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.SharedRopes;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

public class BItCTest {
    static final Term[] INTS;
    static {
        Term[] ints = new Term[1000];
        for (int i = 0; i < ints.length; i++)
            ints[i] = Term.typed(i, SharedRopes.DT_integer);
        INTS = ints;
    }


    static final Vars X = Vars.of("x");

    protected final CallbackBIt<TermBatch> it;
    protected final TermBatch[] batches;
    protected final int[] consumed;
    private final int resultBytes;
    protected int consumedSize = 0;

    private static final IllegalArgumentException LINKED_EX
            = new IllegalArgumentException("linked batches are not supported");

    public BItCTest(CallbackBIt<TermBatch> it, Orphan<TermBatch>[] batches) {
        this.it = it;
        this.batches = new TermBatch[batches.length];
        int consumedCapacity = 0;
        int resultBytes = 0;
        for (var orphan : batches) {
            TermBatch b = orphan.takeOwnership(this);
            if (b.next != null)
                throw LINKED_EX;
            consumedCapacity += b.rows;
            for (int r = 0; r < b.rows; r++)
                resultBytes += requireNonNull(b.get(r, 0)).local().len-1;
        }
        resultBytes += consumedCapacity; // one , per item (including last)
        this.resultBytes = resultBytes;
        this.consumed = new int[consumedCapacity];
    }

    static Orphan<TermBatch> batch(int i) {
        TermBatch b = TermBatchType.TERM.create(1).takeOwnership(BItCTest.class);
        b.beginPut();
        b.putTerm(0, INTS[i]);
        b.commitPut();
        return b.releaseOwnership(BItCTest.class);
    }

    static int parseInt(@Nullable Term term) {
        int value = 0;
        if (term == null)
            return -1;
        var local = term.local();
        for (int mul = 1, i = local.len-1; i > 0; --i, mul *= 10)
            value += mul*(local.get(i)-'0');
        return value;
    }

    protected void produceAndComplete(CallbackBIt<TermBatch> it, TermBatch[] batches) {
        for (TermBatch batch : batches)
            try {
                it.offer(batch.releaseOwnership(this));
            } catch (BatchQueue.QueueStateException ignored) {}
        it.complete(null);
    }

    protected void produceAndComplete() {
        produceAndComplete(it, batches);
    }

    protected void consumeToCompletion() {
        int i = 0;
        try (var g = new Guard.BatchGuard<TermBatch>(this)) {
            for (TermBatch b; (b = g.nextBatch(it)) != null; ) {
                for (; b != null; b = b.next) {
                    for (int r = 0; r < b.rows; r++)
                        consumed[i++] = parseInt(b.get(r, 0));
                }
            }

        }
        consumedSize = i;
    }

    protected String buildResult() {
        var sb = new StringBuilder(resultBytes);
        for (int i = 0; i < consumedSize; i++)
            sb.append(consumed[i]).append(',');
        sb.setLength(Math.max(0, sb.length()-1));
        return sb.toString();
    }

}
