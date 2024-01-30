package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.SharedRopes;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
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

    public BItCTest(CallbackBIt<TermBatch> it, TermBatch[] batches) {
        this.it = it;
        this.batches = batches;
        int consumedCapacity = 0;
        int resultBytes = 0;
        for (TermBatch b : batches) {
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

    static TermBatch batch(int i) {
        TermBatch b = TermBatchType.TERM.create(1);
        b.beginPut();
        b.putTerm(0, INTS[i]);
        b.commitPut();
        return b;
    }

    static void offerAndInvalidate(CallbackBIt<TermBatch> it, TermBatch b) {
        TermBatch mine;
        try {
            mine = it.offer(b);
        } catch (BatchQueue.TerminatedException|BatchQueue.CancelledException e) {
            mine = b;
        }
        if (mine != null) {
            mine.clear();
            mine.beginPut();
            mine.putTerm(0, INTS[999]);
            mine.commitPut();
        }
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

    protected static void produceAndComplete(CallbackBIt<TermBatch> it, TermBatch[] batches) {
        for (TermBatch batch : batches)
            offerAndInvalidate(it, batch);
        it.complete(null);
    }

    protected void produceAndComplete() {
        produceAndComplete(it, batches);
    }

    protected void consumeToCompletion() {
        int i = 0;
        for (TermBatch b = null; (b = it.nextBatch(b)) != null; ) {
            for (var n = b; n != null; n = n.next) {
                for (int r = 0; r < n.rows; r++)
                    consumed[i++] = parseInt(n.get(r, 0));
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
