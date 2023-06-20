package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;

import static java.lang.Runtime.getRuntime;

public class ThreadAffinityPool<T> {
    private static final VarHandle SIZES = MethodHandles.arrayElementVarHandle(int[].class);
    private final T[] data;
    private final int[] sizes;
    private final int bucketMask, bucketSize;

    public ThreadAffinityPool(Class<T> cls, int capacityPerBucket) {
        this(cls, getRuntime().availableProcessors(), capacityPerBucket);
    }

    public ThreadAffinityPool(Class<T> cls, int threadBuckets, int capacityPerBucket) {
        threadBuckets = Math.max(2, 1 << 31-Integer.numberOfLeadingZeros(threadBuckets));
        //noinspection unchecked
        data = (T[]) Array.newInstance(cls, threadBuckets*capacityPerBucket);
        sizes = new int[threadBuckets];
        bucketMask = threadBuckets-1;
        bucketSize = capacityPerBucket;
    }

    public boolean offer(@Nullable T o) {
        if (o == null) return true;
        int bucket = (int)Thread.currentThread().threadId() & bucketMask;
        int size;
        while ((size = (int)SIZES.getAndSetAcquire(sizes, bucket, -1)) == -1)
            Thread.onSpinWait();
        try {
            if (size >= bucketSize) return false;
            data[bucket * bucketSize + size++] = o;
            return true;
        } finally {
            SIZES.setRelease(sizes, bucket, size);
        }
    }

    public @Nullable T get() {
        int bucket = (int)Thread.currentThread().threadId() & bucketMask;
        int size;
        while ((size = (int)SIZES.getAndSetAcquire(sizes, bucket, -1)) == -1)
            Thread.onSpinWait();
        try {
            return size <= 0 ? null : data[bucket*bucketSize + --size];
        } finally {
            SIZES.setRelease(sizes, bucket, size);
        }
    }
}
