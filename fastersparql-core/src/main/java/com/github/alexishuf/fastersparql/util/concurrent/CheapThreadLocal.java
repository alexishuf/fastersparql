package com.github.alexishuf.fastersparql.util.concurrent;

import java.util.function.Supplier;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Runtime.getRuntime;

public class CheapThreadLocal<T> {
    public static final int DEF_CAPACITY =
            1 << (32-numberOfLeadingZeros(getRuntime().availableProcessors()*2-1));
    private final Holder<T>[] holders;
    private final int mask;
    private final Supplier<T> supplier;

    public CheapThreadLocal(Supplier<T> supplier) {
        this(DEF_CAPACITY, supplier);
    }
    public CheapThreadLocal(int capacity, Supplier<T> supplier) {
        this.supplier = supplier;
        //noinspection unchecked
        this.holders = new Holder[capacity];
        this.mask = capacity - 1;
    }
    
    public T get() {
        Thread thread = Thread.currentThread();
        int slot = (int) thread.threadId() & mask;
        Holder<T> h = holders[slot];
        if (h == null || h.owner != thread)
            holders[slot] = h = new Holder<>(thread, supplier.get());
        return h.object;
    }

    private record Holder<T>(Thread owner, T object) { }
}
