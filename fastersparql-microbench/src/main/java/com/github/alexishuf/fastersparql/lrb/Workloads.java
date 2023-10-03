package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.lrb.query.QueryName;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.lrb.query.QueryName.*;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("unchecked")
public class Workloads {
    private static final List<QueryName> SMALL = List.of(S2, S3, S4, S5, S7, C2, C3);
    private static final List<QueryName> MID_SIZE = List.of(C6, C7, C10, S1, S6, S9, S11, S12, S13);
    private static final List<QueryName> BIG = List.of(C8, S8, S10, S14);
    private static final List<QueryName> SMALL_AND_MID = Stream.concat(SMALL.stream(), MID_SIZE.stream()).toList();
    private static final List<QueryName> ALL = Stream.concat(SMALL_AND_MID.stream(), BIG.stream()).toList();

    public static <B extends Batch<B>> List<B> fromName(BatchType<?> t, String name) {
        return switch (name) {
            case "SMALL" -> small(t);
            case "MID", "MID_SIZE", "MID_SIZED" -> mid(t);
            case "SMALL_AND_MID", "MID_AND_SMALL", "SMALL_AND_MID_SIZE", "SMALL_AND_MID_SIZED"
                -> midAndSmall(t);
            case "BIG", "LARGE" -> big(t);
            case "ALL" -> all(t);
            default -> throw new IllegalArgumentException();
        };
    }
    public static <B extends Batch<B>> List<B> small(BatchType<?> t)       { return (List<B>)batches(t, SMALL); }
    public static <B extends Batch<B>> List<B> mid(BatchType<?> t)         { return (List<B>)batches(t, MID_SIZE); }
    public static <B extends Batch<B>> List<B> midAndSmall(BatchType<?> t) { return (List<B>)batches(t, SMALL_AND_MID); }
    public static <B extends Batch<B>> List<B> big(BatchType<?> t)         { return (List<B>)batches(t, BIG); }
    public static <B extends Batch<B>> List<B> all(BatchType<?> t)         { return (List<B>)batches(t, ALL); }

    public static <B extends Batch<B>> List<B> batches(BatchType<B> type, List<QueryName> queries) {
        List<B> list = new ArrayList<>();
        for (QueryName qry : queries) {
            B b = qry.expected(type);
            if (b != null) list.add(b);
        }
        return list;
    }

    public static <B extends Batch<B>> Vars makeVars(List<B> batches) {
        int cols = batches.get(0).cols;
        Vars vars = new Vars.Mutable(cols);
        for (int i = 0; i < cols; i++) vars.add(Rope.of("x", i));
        return vars;
    }

    public static <B extends Batch<B>> List<B> uniformCols(List<B> batches, BatchType<B> bt) {
        int maxCols = 0;
        for (B b : batches)
            if (b.cols > maxCols) maxCols = b.cols;
        Vars out = new Vars.Mutable(maxCols), in = new Vars.Mutable(maxCols);
        for (int i = 0; i < maxCols; i++) out.add(Rope.of("x", i));
        List<B> result = new ArrayList<>(batches.size());
        for (B b : batches) {
            if (b.cols != maxCols) {
                in.clear();
                for (int i = 0; i < b.cols; i++) in.add(Rope.of("x", i));
                b = requireNonNull(bt.projector(out, in)).project(null, b);
            }
            result.add(b);
        }
        return result;
    }

    public static <B extends Batch<B>> void
    repeat(List<B> seed, int n, Collection<List<B>> dest) {
        long last = Timestamp.nanoTime();
        for (int i = 0; i < n; i++) {
            if (Timestamp.nanoTime()-last > 10_000_000_000L) {
                System.out.printf("Creating duplicates of %d batches: %d/%d...\n",
                        seed.size(), i, n);
                last = Timestamp.nanoTime();
            }
            List<B> copy = new ArrayList<>();
            for (B b : seed)
                copy.add(b.copy(null));
            dest.add(copy);
        }
    }

    public static BatchType<?> parseBatchType(String name) {
        return switch (name) {
            case "COMPRESSED", "COMPR", "COMP" -> Batch.COMPRESSED;
            case "TERM", "ARRAY" -> Batch.TERM;
            default -> throw new IllegalArgumentException();
        };
    }

    public static void cooldown(int ms) {
        long start = Timestamp.nanoTime();
        System.gc();
        ms = Math.max(50, ms - (int)((Timestamp.nanoTime()-start)/1_000_000));
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }
}
