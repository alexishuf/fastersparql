package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.IntList;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opentest4j.AssertionFailedError;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;

public class IntsBatch {
    public static final Vars X = Vars.of("x");
    private static final Term[] TERM_POOL = new Term[8192];
    private static final int[][] INT_SEQUENCES = new int[8192][];

    static {
        for (int i = 0; i < 128; i++) {
            TERM_POOL[i] = Term.typed(i, DT_integer);
            INT_SEQUENCES[i] = new int[i];
            for (int j = 0; j < i; j++) INT_SEQUENCES[i][j] = j;
        }
    }

    public static int[] ints(int size) {
        int[] arr = size < INT_SEQUENCES.length ? INT_SEQUENCES[size] : null;
        if (arr == null) {
            arr = new int[size];
            for (int i = 0; i < arr.length; i++) arr[i] = i;
            if (size < INT_SEQUENCES.length) INT_SEQUENCES[size] = arr;
        }
        return arr;
    }

    public static int[] ints(int begin, int size) {
        if (begin == 0) return ints(size);
        int[] arr = new int[size];
        for (int i = 0; i < arr.length; i++) arr[i] = begin+i;
        return arr;
    }

    public static Term term(int i) {
        boolean pooled = i >= 0 && i < TERM_POOL.length;
        Term t = pooled ? TERM_POOL[i] : null;
        if (t == null) {
            t = Term.typed(i, DT_integer);
            if (pooled)
                TERM_POOL[i] = t;
        }
        return t;
    }

    public static int parse(@Nullable Term t) {
        if (t == null)
            throw new IllegalArgumentException("Expected non-null Term");
        SegmentRope local = t.local();
        int val = 0, i = local.len-1;
        int begin = local.len > 2 && local.get(1) == '+' || local.get(1) == '-' ? 2 : 1;
        int m = local.len > 2 && local.get(1) == '-' ? -1 : 1;
        for (; i >= begin ; i--, m *= 10) {
            if (local.get(i) < '0' || local.get(i) > '9')
                throw new IllegalArgumentException("Expected xsd:integer, got "+t);
            val += m * (local.get(i)-'0');
        }
        return val;
    }

    public static TermBatch fill(TermBatch dest, int... ints) {
        for (int i : ints) {
            dest.beginPut();
            dest.putTerm(0, term(i));
            dest.commitPut();
        }
        return dest;
    }

    private static final StaticMethodOwner INTS_BATCH = new StaticMethodOwner("IntsBatch");

    public static Orphan<TermBatch> intsBatch(int... ints) {
        return fill(TERM.create(1).takeOwnership(INTS_BATCH), ints).releaseOwnership(INTS_BATCH);
    }

    public static void offer(CallbackBIt<TermBatch> it, int... ints) {
        offer(it, intsBatch(ints));
    }

    public static void offer(CallbackBIt<TermBatch> it, Orphan<TermBatch> b) {
        try {
            it.offer(b);
        } catch (TerminatedException|CancelledException ignored) {}
    }

    public static int[] histogram(int[] ints, int size) {
        int max = -1;
        for (int i = 0; i < size; i++) max = Math.max(max, ints[i]);
        int[] histogram = new int[max + 1];
        for (int i = 0; i < size; i++) histogram[ints[i]]++;
        return histogram;
    }

    public static int[] histogram(IntList ints) {
        int max = -1;
        for (var it = ints.iterator(); it.hasNext(); ) max = Math.max(max, it.nextInt());
        int[] histogram = new int[max + 1];
        for (var it = ints.iterator(); it.hasNext(); ) histogram[it.nextInt()]++;
        return histogram;
    }

    public static void assertEqualsOrdered(int[] expected, IntList actual) throws AssertionFailedError {
        if (actual.size() != expected.length)
            throw new AssertionFailedError("Size mismatch", expected.length, actual.size());
        var it = actual.iterator();
        for (int i = 0; i < expected.length; i++) {
            if (!it.hasNext())
                throw new AssertionFailedError("Premature end of actual at index "+i);
            int ac = it.nextInt(), ex = expected[i];
            if (ac != expected[i])
                throw new AssertionFailedError("values mismatch at index "+i, ex, ac);
        }
    }
    public static void assertEqualsUnordered(int[] expected, IntList actual,
                                             boolean tolerateDuplicates,
                                             boolean tolerateDeduplicated,
                                             boolean tolerateMissing)  {
        int[] exf = histogram(expected, expected.length);
        int[] acf = histogram(actual);
        for (int i = 0; i < exf.length; i++) {
            int ex = exf[i], ac = i < acf.length ? acf[i] : 0;
            if (ac > ex &&     tolerateDuplicates && ex > 0                     ) continue;
            if (ac < ex && ((tolerateDeduplicated && ac > 0) || tolerateMissing)) continue;
            if (ex != ac) {
                var msg = "Frequency mismatch for item "+i+": expected "+ex+", got "+ac;
                throw new AssertionFailedError(msg, Arrays.toString(expected), actual);
            }
        }
        if (acf.length > exf.length) {
            StringBuilder sb = new StringBuilder("Unexpected values: [");
            for (int i = exf.length; i < acf.length; i++)
                sb.append(i).append(", ");
            sb.setLength(sb.length()-2);
            sb.append("]");
            throw new AssertionFailedError(sb.toString(), Arrays.toString(expected), actual);
        }
    }

}
