package com.github.alexishuf.fastersparql.emit.async;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadPoolsPartitioner {
    private static final ReentrantLock LOCK = new ReentrantLock();
    private static final int LOGICAL_CORES = Runtime.getRuntime().availableProcessors();
    private static final ArrayList<String> NAMES = new ArrayList<>();
    private static int NEXT_PARTITION = 0;


    /**
     * Increments the count of global thread pools that will call {@link #partitionSize()}
     * in the future. This should only be called once per future {@link #partitionSize()}
     * call.
     *
     * <p>Thread pools which are dynamically created (i.e. not held directly or indirectly
     * by a {@code static final} field) should never call this.</p>
     */
    public static void registerPartition(String name) {
        LOCK.lock();
        try {
            if (!NAMES.contains(name))
                NAMES.add(name);
        } finally { LOCK.unlock(); }
    }

    /**
     * The number of logical threads this system CPUs support, divided by the number of
     * previous {@link #registerPartition(String)} calls that had a unique name.
     *
     * <p>In order for the result of this method to be fair, all global thread pools that
     * call it must call {@link #registerPartition(String)} before any of them calls this method.</p>
     *
     * <p>Thread pools which are dynamically created (i.e. not held directly or indirectly
     * by a {@code static final} field) should never call this.</p>
     *
     * @return {@code minThreads} or the number of logical processors divided by the number of
     *         previous {@link #registerPartition(String)} calls.
     */
    public static int partitionSize() {
        return Math.max(1, LOGICAL_CORES/NAMES.size());
    }

    public static BitSet nextLogicalCoreSet() {
        LOCK.lock();
        try {
            var bitset        = new BitSet();
            int partitionSize = partitionSize();
            int start = ((NEXT_PARTITION++)*partitionSize)%LOGICAL_CORES;
            bitset.set(start, start+partitionSize);
            assert bitset.cardinality() == partitionSize;
            return bitset;
        } finally { LOCK.unlock(); }
    }
}
