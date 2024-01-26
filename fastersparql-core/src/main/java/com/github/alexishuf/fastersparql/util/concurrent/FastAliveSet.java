package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Collection;
import java.util.function.Consumer;

/**
 * A fast collection of objects that:
 * <ul>
 *     <li>uses identity semantics even for objects that override {@link Object#equals(Object)} and {@link Object#hashCode()}</li>
 *     <li>works like a hash-map but never performs re-hashing</li>
 *     <li>does not enforce distinctness of elements</li>
 * </ul>
 * @param <T>
 */
public class FastAliveSet<T> {
    private static final Node LOCKED = new Node();
    private static final VarHandle B = MethodHandles.arrayElementVarHandle(Node[].class);
    private final Node[] buckets;
    private final int bucketsMask;

    private static final class Node {
        private Object v0, v1, v2, v3, v4;
        private @Nullable Node next;
    }

    public FastAliveSet(int buckets) {
        buckets = 1 << Math.min(16, 32 - Integer.numberOfLeadingZeros(buckets-1));
        this.bucketsMask = buckets-1;
        this.buckets = new Node[buckets];
    }
    
    public void add(T o) {
        if (o == null)
            return;
        int bIdx = System.identityHashCode(o) & bucketsMask;
        Node root, n, last;
        while ((root=(Node)B.getAndSetAcquire(buckets, bIdx, LOCKED)) == LOCKED)
            Thread.onSpinWait();
        try {
            for (n = last = root; n != null; n = (last=n).next) { // scan for first open slot
                if        (n.v0 == null) {
                    n.v0 = o; return;
                } else if (n.v1 == null) {
                    n.v1 = o; return;
                } else if (n.v2 == null) {
                    n.v2 = o; return;
                } else if (n.v3 == null) {
                    n.v3 = o; return;
                } else if (n.v4 == null) {
                    n.v4 = o; return;
                }
            }
            // no open slot in linked Node list, append a new Node
            if (last == null) last = root = new Node();
            else              last = (last.next = new Node());
            last.v0 = o;
        } finally {
            B.setRelease(buckets, bIdx, root);
        }
    }

    public void remove(Object o) {
        if (o == null)
            return;
        int bucketIdx = System.identityHashCode(o)&bucketsMask;
        Node root;
        while ((root=(Node)B.getAndSetAcquire(buckets, bucketIdx, LOCKED)) == LOCKED)
            Thread.onSpinWait();
        try {
            for (var n = root; o != null && n != null; n = n.next) {
                if      (n.v0 == o) n.v0 = o = null;
                else if (n.v1 == o) n.v1 = o = null;
                else if (n.v2 == o) n.v2 = o = null;
                else if (n.v3 == o) n.v3 = o = null;
                else if (n.v4 == o) n.v4 = o = null;
            }
        } finally {
            B.setRelease(buckets, bucketIdx, root);
        }
    }

    /**
     * Safely execute {@code consumer.accept(o)} for every object in this set and remove said
     * items from this set. Behaves as a {@link java.util.Collection#forEach(Consumer)}
     * followed by {@link Collection#clear()}
     *
     * @param consumer a function to execute for every item. This function should not cause
     *                 new {@link #add(Object)} calls in this set, else this method may
     *                 never return.
     */
    @SuppressWarnings("unchecked") public void destruct(Consumer<T> consumer) {
        for (boolean hasItems = true; hasItems; ) {
            for (int i = 0; i < buckets.length; i++) {
                Node n;
                while ((n=(Node)B.getAndSetAcquire(buckets, i, null)) == LOCKED)
                    Thread.onSpinWait();
                for (; n != null; n = n.next) {
                    T v;
                    if ((v=(T)n.v0) != null) consumer.accept(v);
                    if ((v=(T)n.v1) != null) consumer.accept(v);
                    if ((v=(T)n.v2) != null) consumer.accept(v);
                    if ((v=(T)n.v3) != null) consumer.accept(v);
                    if ((v=(T)n.v4) != null) consumer.accept(v);
                }
                hasItems = B.getAcquire(buckets, i) != null;
            }
        }
    }
}
