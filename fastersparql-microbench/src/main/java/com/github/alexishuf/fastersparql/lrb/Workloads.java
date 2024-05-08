package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.lrb.query.QueryName;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.RopeFactory;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.lrb.query.QueryName.*;

@SuppressWarnings("unchecked")
public class Workloads {
    private static final List<QueryName> SMALL = List.of(S2, S3, S4, S5, S7, C2, C3);
    private static final List<QueryName> MID_SIZE = List.of(C6, C7, C10, S1, S6, S9, S11, S12, S13);
    private static final List<QueryName> BIG = List.of(C8, S8, S10, S14);
    private static final List<QueryName> SMALL_AND_MID = Stream.concat(SMALL.stream(), MID_SIZE.stream()).toList();
    private static final List<QueryName> ALL = Stream.concat(SMALL_AND_MID.stream(), BIG.stream()).toList();

    public static <B extends Batch<B>> Orphan<B> fromName(BatchType<?> t, String name) {
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
    public static <B extends Batch<B>> B small(BatchType<?> t)       { return (B)batches(t, SMALL); }
    public static <B extends Batch<B>> B mid(BatchType<?> t)         { return (B)batches(t, MID_SIZE); }
    public static <B extends Batch<B>> B midAndSmall(BatchType<?> t) { return (B)batches(t, SMALL_AND_MID); }
    public static <B extends Batch<B>> B big(BatchType<?> t)         { return (B)batches(t, BIG); }
    public static <B extends Batch<B>> B all(BatchType<?> t)         { return (B)batches(t, ALL); }

    public static <B extends Batch<B>> B batches(BatchType<B> type, List<QueryName> queries) {
        int width = 0;
        for (QueryName qry : queries) {
            B b = qry.expected(type);
            width = Math.max(width, b == null ? 0 : b.cols);
        }
        B queue =  type.create(width).takeOwnership(BATCHES);
        Vars outVars = makeVars(width);
        for (QueryName qry : queries) {
            B b = qry.expected(type);
            if (b == null)
                continue;
            Orphan<B> orphan = b.dup();
            if (b.cols < width) {
                var pOrphan = type.projector(outVars, makeVars(b.cols));
                assert pOrphan != null;
                var p  = pOrphan.takeOwnership(BATCHES);
                orphan = p.projectInPlace(orphan);
                p.recycle(BATCHES);
            }
            Batch.quickAppend(queue, BATCHES, orphan);
        }
        return queue;
    }
    private static final StaticMethodOwner BATCHES = new StaticMethodOwner("Workloads.batches");

    public static Vars makeVars(Batch<?> queue) {
        return makeVars(queue.cols);
    }
    public static Vars makeVars(int cols) {
        Vars vars = new Vars.Mutable(cols);
        for (int i = 0; i < cols; i++)
            vars.add(RopeFactory.make(4).add('x').add(i).take());
        return vars;
    }

    public static <B extends Batch<B>> void
    repeat(B seed, int n, Collection<? super B> dest, Object destOwner) {
        int totalRows = seed.totalRows();
        long last = Timestamp.nanoTime();
        for (int i = 0; i < n; i++) {
            if (Timestamp.nanoTime()-last > 10_000_000_000L) {
                System.out.printf("Creating duplicates of queue with %d rows: %d/%d...\n",
                        totalRows, i, n);
                last = Timestamp.nanoTime();
            }
            dest.add(seed.dup().takeOwnership(destOwner));
        }
    }

    public static BatchType<?> parseBatchType(String name) {
        return switch (name) {
            case "COMPRESSED", "COMPR", "COMP" -> CompressedBatchType.COMPRESSED;
            case "TERM", "ARRAY" -> TermBatchType.TERM;
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
