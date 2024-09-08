package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.Rope;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class IdHashCache {
    private static final VarHandle D = MethodHandles.arrayElementVarHandle(long[].class);
    private static final long LOCKED = 0xefffffff00000000L;
    @SuppressWarnings("RedundantCast") private static final long NULL_HASH = (long)Rope.FNV_BASIS;

    public static final int BUCKET_BITS  = 20;
    public static final int BUCKET_COUNT = 1<< BUCKET_BITS;
    public static final int BUCKET_MASK  = BUCKET_COUNT-1;

    static {assert Integer.bitCount(BUCKET_COUNT) == 1 : "not a power-of-2" ;}

    public static long[] create(long nullId) {
        long[] data = new long[BUCKET_COUNT<<1];
        for (int i = 0; i < data.length; i += 2) {
            data[i  ] = nullId;
            data[i+1] = NULL_HASH;
        }
        return data;
    }

    public static void set(long[] data, long sourcedId, int bucket, int hash) {
        int base = (bucket&BUCKET_MASK)<<1;
        long ac = (long)D.getAndSetAcquire(data, base+1, LOCKED);
        if (ac != LOCKED)
            return; // lost race
        data[base] = sourcedId;
        D.setRelease(data, base+1, (long)hash);
    }

    public static int get(long[] data, long sourcedId, int bucket, int hashIfNotPresent) {
        int base = (bucket&BUCKET_MASK)<<1;
        if ((long)D.getAcquire(data, base) != sourcedId)
            return hashIfNotPresent; // cache miss
        long hash = (long)D.getAcquire(data, base+1);
        if (hash == LOCKED)
            return hashIfNotPresent; // concurrent write
        return (long)D.getAcquire(data, base) == sourcedId ? (int)hash : hashIfNotPresent;
    }
}
