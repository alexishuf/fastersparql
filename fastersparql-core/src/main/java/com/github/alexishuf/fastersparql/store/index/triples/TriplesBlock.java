package com.github.alexishuf.fastersparql.store.index.triples;

import com.github.alexishuf.fastersparql.store.index.Sorter;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentScope;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;
import static jdk.incubator.vector.LongVector.fromMemorySegment;

public class TriplesBlock implements Sorter.BlockJob<TriplesBlock> {
    private static final Logger log = LoggerFactory.getLogger(TriplesBlock.class);
    private static final VectorSpecies<Long> L_SP = LongVector.SPECIES_PREFERRED;
    private static final boolean VEC_SHUFFLE = L_SP.length() == 4;

    final MemorySegment seg;
    private final Path path;
    private final FileChannel ch;
    int triples;

    public TriplesBlock(Path file, SegmentScope scope, int triplesCapacity) throws IOException {
        path = file;
        ch = FileChannel.open(path, CREATE, READ, WRITE, TRUNCATE_EXISTING);
        seg = ch.map(READ_WRITE, 0, triplesCapacity * 24L, scope);
    }

    @Override public void close() {
        try {
            seg.unload();
            ch.close();
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.error("Failed to close/delete {}", path, e);
        }
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder().append("TriplesBlock@")
                .append(Integer.toString(System.identityHashCode(this), 16))
                .append('[');
        for (int i = 0, n = 24*Math.min(10, triples); i < n; i+=24) {
            sb.append('(');
            sb.append(seg.get(JAVA_LONG, i)).append(' ');
            sb.append(seg.get(JAVA_LONG, i+8)).append(' ');
            sb.append(seg.get(JAVA_LONG, i+16)).append("), ");
        }
        if (triples <= 10) sb.setLength(sb.length() - 2);
        else               sb.append('+').append(triples - 10);
        sb.append(']');
        return sb.toString();
    }

    @Override public boolean      isEmpty() { return triples == 0; }
    @Override public void         reset()   { triples = 0; }
    @Override public TriplesBlock run()     { sort(0, triples); return this; }

    public int tripleCount() { return triples; }

    public boolean add(long s, long p, long o) {
        int triples = this.triples, out = triples * 24;
        if (out+24 > seg.byteSize())
            return false;
        seg.set(JAVA_LONG, out, s);
        seg.set(JAVA_LONG, out + 8, p);
        seg.set(JAVA_LONG, out + 16, o);
        this.triples = triples + 1;
        return true;
    }

    private static final VectorShuffle<Long> SPO2PSO = L_SP.shuffleFromValues(1, 0, 2, 3);
    private static final VectorMask<Long> T_MASK = VectorMask.fromValues(L_SP, true, true, true, false);

    /**
     * Does an in-place quicksort of triples in {@code [begin, end)}. Triples will be
     * compared lexicographically.
     */
    public void sort(int begin, int end) {
        if (end-begin < 2) return; // already sorted
        int mid = partition(begin, end-1);
        sort(begin, mid+1);
        sort(mid+1, end);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private int partition(int fst, int lst) {
        int mid = (fst + lst) >>> 1, i = mid*24, j;
        long pv0 = seg.get(JAVA_LONG, i);
        long pv1 = seg.get(JAVA_LONG, i+8);
        long pv2 = seg.get(JAVA_LONG, i+16);

        // compensate for infix decrement on whiles below. the infix ensures the
        // algorithm progresses when *fst == *lst
        --fst;
        ++lst;
        while (true) {
            // move fst and lst pointers toward mid while *fst < *mid and *lst > *mid
            while (cmp(++fst, pv0, pv1, pv2) < 0) { /* pass */ }
            while (cmp(--lst, pv0, pv1, pv2) > 0) { /* pass */ }
            if (lst <= fst) //finished scanning both partitions
                return lst;

            // triples at fst and lst are on the wrong side, swap
            i = fst*24; j = lst*24;
            long tmp = seg.get(JAVA_LONG, i);
            seg.set(JAVA_LONG, i, seg.get(JAVA_LONG, j));
            seg.set(JAVA_LONG, j, tmp);

            i += 8; j += 8;
            tmp = seg.get(JAVA_LONG, i);
            seg.set(JAVA_LONG, i, seg.get(JAVA_LONG, j));
            seg.set(JAVA_LONG, j, tmp);

            i += 8; j += 8;
            tmp = seg.get(JAVA_LONG, i);
            seg.set(JAVA_LONG, i, seg.get(JAVA_LONG, j));
            seg.set(JAVA_LONG, j, tmp);
        }
    }

    public int cmp(int triple, long s, long p, long o) {
        triple *= 24;
        int diff = Long.signum(seg.get(JAVA_LONG, triple) - s);
        if (diff != 0) return diff;
        diff = Long.signum(seg.get(JAVA_LONG, triple+8) - p);
        if (diff != 0) return diff;
        return Long.signum(seg.get(JAVA_LONG, triple+16) - o);
    }

    public void spo2pso() {
        int i = 0;
        if (VEC_SHUFFLE && triples > 10) { // will read 9 longs from 3 triples + 1 long
            // SPO SPO SPO S // triple roles
            // 012 345 678   // long index
            // ___ _         // v0
            //     ___ _     // v1
            //         ___ _ // v2
            i = 72;
            for (int ve = L_SP.loopBound(triples * 3) - 3; i < ve; i += 72) {
                var v0 = fromMemorySegment(L_SP, seg, i - 72, LITTLE_ENDIAN);
                var v1 = fromMemorySegment(L_SP, seg, i - 48, LITTLE_ENDIAN);
                var v2 = fromMemorySegment(L_SP, seg, i - 24, LITTLE_ENDIAN);
                v0.rearrange(SPO2PSO).intoMemorySegment(seg, i - 72, LITTLE_ENDIAN, T_MASK);
                v1.rearrange(SPO2PSO).intoMemorySegment(seg, i - 48, LITTLE_ENDIAN, T_MASK);
                v2.rearrange(SPO2PSO).intoMemorySegment(seg, i - 24, LITTLE_ENDIAN, T_MASK);
            }
            i -= 72;
        }
        for (int end = triples * 24; i < end; i += 24) {
            long tmp = seg.get(JAVA_LONG, i);
            seg.set(JAVA_LONG, i, seg.get(JAVA_LONG, i + 8));
            seg.set(JAVA_LONG, i + 8, tmp);
        }
    }

    private static final VectorShuffle<Long> PSO2OPS = L_SP.shuffleFromValues(2, 0, 1, 3);
    public void pso2ops() {
        int i = 0;
        if (VEC_SHUFFLE && triples > 10) {
            i = 72;
            for (int ve = L_SP.loopBound(triples * 3); i < ve; i += 72) {
                var v0 = fromMemorySegment(L_SP, seg, i - 72, LITTLE_ENDIAN);
                var v1 = fromMemorySegment(L_SP, seg, i - 48, LITTLE_ENDIAN);
                var v2 = fromMemorySegment(L_SP, seg, i - 24, LITTLE_ENDIAN);
                v0.rearrange(PSO2OPS).intoMemorySegment(seg, i - 72, LITTLE_ENDIAN);
                v1.rearrange(PSO2OPS).intoMemorySegment(seg, i - 48, LITTLE_ENDIAN);
                v2.rearrange(PSO2OPS).intoMemorySegment(seg, i - 24, LITTLE_ENDIAN);
            }
            i -= 72;
        }
        for (int end = triples * 24; i < end; i += 24) {
            long p = seg.get(JAVA_LONG, i);
            long s = seg.get(JAVA_LONG, i + 8);
            long o = seg.get(JAVA_LONG, i + 16);
            seg.set(JAVA_LONG, i, o);
            seg.set(JAVA_LONG, i + 8, p);
            seg.set(JAVA_LONG, i + 16, s);
        }
    }
}
