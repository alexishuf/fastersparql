package com.github.alexishuf.fastersparql.model;

import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc;
import com.github.alexishuf.fastersparql.util.concurrent.LevelAlloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.lang.System.arraycopy;
import static java.util.Arrays.binarySearch;

public abstract sealed class RopeArrayMap extends AbstractOwned<RopeArrayMap> {
    private static final int SORT_THRESHOLD = 8;
    private static final Rope[] EMPTY_DATA = new Rope[0];
    private static final ArrayAlloc<Rope[]> DATA_ALLOC = new ArrayAlloc<>(Rope[].class,
            "RopeArrayMap.DATA_ALLOC", 4,
            new LevelAlloc.Capacities()
                    .set(4, 4, Alloc.THREADS*64)
                    .setSameBytesUsage(5, 10, Alloc.THREADS*64*(20+32*4),
                                       20, 4)
                    .set(11, 13, Alloc.THREADS)
    );
    static {
        Primer.INSTANCE.sched(() -> {
            for (int level = 0; level <= 8; level++)
                DATA_ALLOC.primeLevel(level);
        });
    }

    private Rope[] data;
    private int keys = 0;

    public static Orphan<RopeArrayMap> create() {
        return new Concrete(DATA_ALLOC.createFromLevel(3));
    }
    public static Orphan<RopeArrayMap> create(Rope[] data) {
        return new Concrete(data);
    }

    private RopeArrayMap(Rope[] data) {this.data = data;}

    @Override public @Nullable RopeArrayMap recycle(Object currentOwner) {
        internalMarkGarbage(currentOwner);
        data = EMPTY_DATA;
        keys = 0;
        return null;
    }

    private static final class Concrete extends RopeArrayMap implements Orphan<RopeArrayMap> {
        private Concrete(Rope[] data) {super(data);}
        @Override public RopeArrayMap takeOwnership(Object o) {return takeOwnership0(o);}
    }

    public int size() { return keys; }

    public void put(FinalSegmentRope key, @Nullable Rope value) {
        requireAlive();
        int size = this.keys;
        if (size > SORT_THRESHOLD) {
            putSorted(key, value);
        } else {
            int i = 0, half = data.length>>1;
            while (i < size && !key.equals(data[i])) ++i;
            if (i == size) { // key not found
                if (i == SORT_THRESHOLD) { // must sort
                    sort();
                    putSorted(key, value);
                    return;
                } else if (size == half) { // must grow
                    half = data.length;
                    grow();
                }
                data[i] = key;
                ++keys;
            }
            data[half+i] = value;
        }
    }

    private void putSorted(Rope key, @Nullable Rope value) {
        int size = keys, half = data.length>>1, i = binarySearch(data, 0, size, key);
        if (i < 0) {
            i = -i - 1;
            if (size >= half) {
                half = data.length;
                grow();
            }
            int tail = size-i;
            if (tail > 0) {
                arraycopy(data, i, data, i+1, tail);
                arraycopy(data, half+i, data, half+i+1, tail);
            }
            data[i] = key;
            ++keys;
        }
        data[half + i] = value;
    }

    public void putAll(RopeArrayMap other) {
        Rope[] d = other.data;
        for (int i = 0, half = d.length>>1, n = other.keys; i < n; i++)
            put((FinalSegmentRope)d[i], d[half+i]);
    }

    public void resetToCopy(RopeArrayMap other) {
        requireAlive();
        int keys = other.keys;
        this.keys = keys;
        if (data.length < keys<<1) {
            DATA_ALLOC.offer(data, data.length);
            data = DATA_ALLOC.createAtLeast(keys<<1);
        }
        Rope[] oData = other.data;
        arraycopy(oData, 0,               data, 0,              keys);
        arraycopy(oData, oData.length>>1, data, data.length>>1, keys);
    }

    public void clear() {
        keys = 0;
    }

    public Rope key(int i) {
        if (i >= keys) throw new IndexOutOfBoundsException(i);
        return data[i];
    }

    public @Nullable Rope value(int i) {
        return i < keys ? data[(data.length>>1) + i] : null;
    }

    public @Nullable Rope get(Rope key) {
        int i = 0, keys = this.keys;
        if (keys > SORT_THRESHOLD) {
            i = binarySearch(data, 0, keys, key);
        } else {
            while (i != keys && !key.equals(data[i])) ++i;
        }
        return i == keys || i < 0 ? null : data[(data.length>>1)+i];
    }

    public @Nullable Rope get(Rope key, int begin, int end) {
        int len = end - begin;
        if (keys > SORT_THRESHOLD) {
            int i = subKeyBinarySearch(key, begin, end);
            return i >= 0 ? data[(data.length>>1) + i] : null;
        }
        for (int i = 0, keys = this.keys; i < keys; i++) {
            Rope candidate = data[i];
            if (candidate.len() == len && key.has(begin, candidate))
                return data[i+(data.length>>1)];
        }
        return null;
    }

    @Override public boolean equals(Object obj) {
        if (obj == this) return true;
        if (!(obj instanceof RopeArrayMap r) || r.keys != keys) return false;
        for (int i = 0, n = keys; i < n; i++) {
            if (!data[i].equals(r.data[i])) return false;
        }
        for (int i = data.length>>1, j = r.data.length>>1, end = i+keys; i < end; i++, ++j) {
            if (!data[i].equals(r.data[j])) return false;
        }
        return true;
    }

    @Override public int hashCode() {
        int h = 0;
        for (int i = 0, n = keys; i < n; i++)
            h = 31*h + data[i].hashCode();
        for (int i = data.length>>1, end = i+keys; i < end; i++)
            h = 31*h + data[i].hashCode();
        return h;
    }

    @Override public String toString() {
        try (var sb = PooledMutableRope.get()) {
            sb.append('{');
            for (int i = 0; i < keys; i++)
                sb.append(data[i]).append('=').append(data[(data.length >> 1) + i]).append(", ");
            if (keys > 0) sb.unAppend(2);
            return sb.append('}').toString();
        }
    }

    /** Equivalent to {@code binarySearch(data, 0, keys, key.sub(keyBegin, keyEnd))} but
     *  without causing instantiations. */
    private int subKeyBinarySearch(Rope key, int keyBegin, int keyEnd) {
        int low = 0, high = keys-1;
        while (low <= high) {
            int i = (low+high)>>>1, diff = data[i].compareTo(key, keyBegin, keyEnd);
            if      (diff < 0) low = i+1;
            else if (diff > 0) high = i-1;
            else               return i;
        }
        return -low-1;
    }

    private void sort() {
        int half = data.length>>1;
        Rope[] next = data.length < SORT_THRESHOLD<<2 ? new Rope[SORT_THRESHOLD<<2] : data;
        arraycopy(data, 0, next, SORT_THRESHOLD, SORT_THRESHOLD);
        arraycopy(data, half, next, (next.length>>1) + SORT_THRESHOLD, SORT_THRESHOLD);
        half = (data = next).length>>1;
        // At this point data looks like this (SORT_THRESHOLD=4, for simplicity):
        // data:  __ __ __ __ k0 k1 k2 k3     __ __ __ __ v0 v1 v2 v3
        // index: 0  1  2  3  4  5  6  7      8  9  10 11 12 13 14 15
        //        |           ↳ unsorted keys |           ↳ unsorted values:
        //        ↳ sorted keys dest          ↳ sorted values destination: data.length>>1:
        keys = 0;
        for (int i = SORT_THRESHOLD, end = SORT_THRESHOLD<<1; i < end; i++)
            putSorted(data[i], data[half+i]);
    }

    private void grow() {
        int half = data.length>>1;
        Rope[] copy = new Rope[2*data.length];
        arraycopy(data, 0, copy, 0, half); // copy keys
        arraycopy(data, half, copy, data.length, half); //copy values
        data = copy;
    }
}
