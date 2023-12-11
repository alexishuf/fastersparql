package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.util.concurrent.AffinityPool;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;

import java.util.Arrays;

public class HashBitset {
    private static final int POOL_CAPACITY = 1024;
    private static final int POOL_THREADS = Runtime.getRuntime().availableProcessors();
    private static final AffinityPool<long[]> POOL
            = new AffinityPool<>(long[].class, POOL_CAPACITY, POOL_THREADS);

    public static final int BS_BITS   = 1<<16;
    public static final int BS_WORDS  = BS_BITS/64;
    public static final int HASH_MASK = BS_BITS-1;

    static {
        assert Integer.bitCount(BS_WORDS) == 1;
        int baseline = 8*Runtime.getRuntime().availableProcessors();
        for (int i = 0; i < baseline; i++)
            POOL.offer(new long[BS_WORDS]);
    }

    public static long[] get() {
        long[] bitset = POOL.get();
        if (bitset == null) return new long[BS_WORDS];
        Arrays.fill(bitset, 0L);
        return bitset;
    }

    @SuppressWarnings("SameReturnValue") public static long[] recycle(long[] bitset) {
        POOL.offer(bitset);
        return ArrayPool.EMPTY_LONG;
    }
}
