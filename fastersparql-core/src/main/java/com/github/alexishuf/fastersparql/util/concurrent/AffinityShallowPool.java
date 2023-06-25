package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static java.lang.Runtime.getRuntime;
import static java.lang.Thread.currentThread;

@SuppressWarnings("unchecked")
public class AffinityShallowPool {
    private static final VarHandle D = MethodHandles.arrayElementVarHandle(Object[].class);
    private static final VarHandle FREE_COLS;
    static {
        try {
            FREE_COLS = MethodHandles.lookup().findStaticVarHandle(AffinityShallowPool.class, "plainFreeCols", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final int WIDTH = 32;
    private static final int W_SHIFT = Integer.numberOfTrailingZeros(WIDTH);
    @SuppressWarnings("FieldMayBeFinal") private static long plainFreeCols = -1;
    public static final int MASK;
    private static final Object[] data;
    static {
        //noinspection ConstantValue
        assert WIDTH < 64;
        assert 1 << W_SHIFT == WIDTH;
        int threads = 32-Integer.numberOfLeadingZeros(getRuntime().availableProcessors()-1);
        assert Integer.bitCount(threads) == 1;
        MASK = threads-1;
        data = new Object[threads*WIDTH];
    }

    public static int reserveColumn() {
        long free;
        int i;
        do {
            free = (long) FREE_COLS.getOpaque();
            if ((i = Long.numberOfTrailingZeros(free)) >= WIDTH)
                throw new IllegalStateException("No columns free");
        } while (!FREE_COLS.compareAndSet(free, free&~(1L << i)));
        return i;
    }

    public static <T> T get(int column) {
        int id = (int) currentThread().threadId();
        T o = (T)D.getAndSetAcquire(data, ((id &MASK)<<W_SHIFT)+column, null);
        if (o != null) return o;
        return (T)D.getAndSetAcquire(data, (((id-1)&MASK)<<W_SHIFT)+column, null);
    }

    public static <T> T get(int column, int threadId) {
        T o = (T) D.getAndSetAcquire(data, (( threadId   &MASK)<<W_SHIFT)+column, null);
        if (o != null) return o;
        return (T)D.getAndSetAcquire(data, (((threadId-1)&MASK)<<W_SHIFT)+column, null);
    }

    public static <T> @Nullable T offer(int column, @Nullable T o) {
        if (o == null) return null;
        int id = (int) currentThread().threadId();
        if (D.compareAndExchangeRelease(data, ((id&MASK) <<W_SHIFT)+column, null, o) == null)
            return null;
        if (D.compareAndExchangeRelease(data, (((id+1)&MASK)<<W_SHIFT)+column, null, o) == null)
            return null;
        return o;
    }

    public static <T> @Nullable T offer(int column, @Nullable T o, int threadId) {
        if (o == null) return null;
        if (D.compareAndExchangeRelease(data, ((threadId&MASK) <<W_SHIFT)+column, null, o) == null)
            return null;
        if (D.compareAndExchangeRelease(data, (((threadId+1)&MASK)<<W_SHIFT)+column, null, o) == null)
            return null;
        return o;
    }

}
