package com.github.alexishuf.fastersparql.util;

import com.github.alexishuf.fastersparql.sparql.expr.Term;

import static com.github.alexishuf.fastersparql.model.rope.RopeSupport.compareTo;

public class BSearch {
    /** Branch-less implementation of {@link java.util.Arrays#binarySearch(int[], int)} */
    public static int binarySearch(int[] sorted, int item) {
        int base = 0, len = sorted.length;
        if (len <= 4) return linearSearch(sorted, base, sorted.length, item);
        for (int half = len>>1; len > 1; half = (len -= half) >> 1)
            base += half * ((sorted[base+half-1] - item) >>> 31);
        int cmp = sorted[base]-item;
        return cmp == 0 ? base : -base + (cmp>>31) - 1;
    }

    /** Branch-less implementation of {@link java.util.Arrays#binarySearch(int[], int, int, int)} */
    public static int binarySearch(int[] sorted, int from, int to, int item) {
        int base = from, len = to-from;
        if (len <= 4) return linearSearch(sorted, base, to, item);
        for (int half = len>>1; len > 1; half = (len -= half) >> 1)
            base += half * ((sorted[base+half-1] - item) >>> 31);
        int cmp = sorted[base]-item;
        return cmp == 0 ? base : -base + (cmp>>31) - 1;
    }

    private static int linearSearch(int[] sorted, int from, int to, int item) {
        for (int i = from; i < to; i++) {
            int cmp = item - sorted[i];
            if      (cmp == 0) return i;
            else if (cmp <  0) return -i-1;
        }
        return -to-1;
    }

    public static int binarySearch(byte[][] sorted, int from, int to, byte[] item) {
        for (int n = to-from, mid; n > 1; n = to-from) {
            byte[] candidate = sorted[mid = from + (n >> 1)];
            int diff = compareTo(candidate, item);
            if      (diff < 0) from = mid+1;
            else if (diff > 0) to = mid;
            else               to = (from = mid) + 1;
        }
        int diff = from == to ? 1 : compareTo(sorted[from], item);
        return diff == 0 ? from : -(from + (diff>>>31))-1;
    }

//    public static int binarySearchLocal(Term[] sorted, int from, int to, Rope key, int localBegin) {
//        if (key instanceof Term t)
//            return binarySearchLocal(sorted, from, to, t.local);
//        byte f = key.get(localBegin);
//        int keyLen = key.len();
//        for (int n = to-from, mid; n > 1; n = to-from) {
//            byte[] candidate = sorted[mid = from + (n >> 1)].local;
//            int diff = candidate[0] - f;
//            if (diff == 0)
//                diff = compareTo(candidate, key, localBegin, keyLen);
//            if      (diff < 0) from = mid+1;
//            else if (diff > 0) to = mid;
//            else               to = (from = mid) + 1;
//        }
//        int diff = from == to ? 1 : compareTo(sorted[from].local, key, localBegin, keyLen);
//        return diff == 0 ? from : -(from + (diff>>>31))-1;
//    }

    public static int binarySearchLocal(Term[] sorted, int from, int to, byte[] local) {
        for (int n = to-from, mid; n > 1; n = to-from) {
            byte[] candidate = sorted[mid = from + (n >> 1)].local;
            int diff = compareTo(candidate, local);
            if      (diff < 0) from = mid+1;
            else if (diff > 0) to = mid;
            else               to = (from = mid) + 1;
        }
        int diff = from == to ? 1 : compareTo(sorted[from].local, local);
        return diff == 0 ? from : -(from + (diff>>>31))-1;
    }
}
