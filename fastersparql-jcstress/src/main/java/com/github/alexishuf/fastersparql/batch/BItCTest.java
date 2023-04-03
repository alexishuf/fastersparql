package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

public class BItCTest {
    static final Term[] INTS;
    static {
        Term[] ints = new Term[1000];
        for (int i = 0; i < ints.length; i++)
            ints[i] = Term.typed(i, RopeDict.DT_integer);
        INTS = ints;
    }


    static final Vars X = Vars.of("x");

    protected final CallbackBIt<TermBatch> it;
    protected final TermBatch[] batches;
    protected final int[] consumed;
    private final int resultBytes;
    protected int consumedSize = 0;

    public BItCTest(CallbackBIt<TermBatch> it, TermBatch[] batches) {
        this.it = it;
        this.batches = batches;
        int consumedCapacity = 0;
        int resultBytes = 0;
        for (TermBatch b : batches) {
            consumedCapacity += b.rows;
            for (int r = 0; r < b.rows; r++)
                resultBytes += requireNonNull(b.get(r, 0)).local.length-1;
        }
        resultBytes += consumedCapacity; // one , per item (including last)
        this.resultBytes = resultBytes;
        this.consumed = new int[consumedCapacity];
    }

    static TermBatch batch(int i) {
        TermBatch b = Batch.TERM.createSingleton(1);
        b.beginPut();
        b.putTerm(INTS[i]);
        b.commitPut();
        return b;
    }

    static void offerAndInvalidate(CallbackBIt<TermBatch> it, TermBatch b) {
        TermBatch mine = it.offer(b);
        if (mine != null) {
            mine.clear();
            mine.beginPut();
            mine.putTerm(INTS[999]);
            mine.commitPut();
        }
    }

    static int parseInt(@Nullable Term term) {
        int value = 0;
        if (term == null)
            return -1;
        byte[] local = term.local;
        for (int mul = 1, i = local.length-1; i > 0; --i, mul *= 10)
            value += mul*(local[i]-'0');
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
            for (int r = 0; r < b.rows; r++)
                consumed[i++] = parseInt(b.get(r, 0));
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