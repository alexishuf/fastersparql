package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;

import java.util.Arrays;
import java.util.function.Supplier;

public class HashBitset {
    public static final int BS_BITS   = 1<<16;
    public static final int BS_WORDS  = BS_BITS/64;
    public static final int HASH_MASK = BS_BITS-1;

    private static final int POOL_CAPACITY = Alloc.THREADS*128;
    private static final  Supplier<long[]> FAC = new Supplier<>() {
        @Override public long[] get() {return new long[BS_WORDS];}
        @Override public String toString() {return "HashBitset.FAC";}
    };
    private static final Alloc<long[]> POOL = new Alloc<>(long[].class,
            "HashBitset.POOL", POOL_CAPACITY, FAC, BS_WORDS*8);

    static {
        assert Integer.bitCount(BS_WORDS) == 1;
        Primer.INSTANCE.sched(POOL::prime);
    }

    public static long[] get() {
        long[] bitset = POOL.poll();
        if (bitset == null) return new long[BS_WORDS];
        Arrays.fill(bitset, 0L);
        return bitset;
    }

    @SuppressWarnings("SameReturnValue") public static long[] recycle(long[] bitset) {
        if (POOL.offer(bitset) != null)
            ArrayAlloc.recycleLongs(bitset);
        return ArrayAlloc.EMPTY_LONG;
    }
}
