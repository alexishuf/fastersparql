package com.github.alexishuf.fastersparql.model;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

import static java.lang.System.arraycopy;
import static java.util.Arrays.binarySearch;

public class RopeArrayMap {
    private static final int SORT_THRESHOLD = 16;

    private Rope[] data;
    private int keys = 0;

    public RopeArrayMap()                 { data  = new Rope[16]; }

    public int size() { return keys; }

    public void put(Rope key, @Nullable Rope value) {
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
            put(d[i], d[half+i]);
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
        int keys = this.keys, len = end - begin;
        if (keys > SORT_THRESHOLD)
            return get(key.sub(begin, end));
        for (int i = 0; i < keys; i++) {
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
        var sb = new ByteRope().append('{');
        for (int i = 0; i < keys; i++)
            sb.append(data[i]).append('=').append(data[(data.length>>1) + i]).append(", ");
        if (keys > 0) sb.unAppend(2);
        return sb.toString();
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
