package com.github.alexishuf.fastersparql.util;

import java.util.Arrays;
import java.util.BitSet;

import static java.lang.Long.bitCount;
import static java.lang.Long.numberOfTrailingZeros;

/**
 * A spartan {@link BitSet}. Differences from {@link BitSet}:
 *
 * <ul>
 *     <li>Avoids the cost of allocating a wrapper object around the int[] array</li>
 *     <li>Avoids the get()/set() method call overhead</li>
 *     <li>int[] arrays are faster to iterate with nextSet/nextClear</li>
 *     <li>{@code nextSet(set, i+1)}  is optimized for returning {@code i+1}</li>
 *     <li>Cardinality and bitwise operation between sets are slower due to the use of
 *         ints instead of longs</li>
 * </ul>
 */
public class BS {
    /**
     * Returns a zero-filled bitset that can hold <strong>at least</strong> {@code size} bits.
     *
     * @param set {@code null} or a bitset that can be zero-filled and returned in case it
     *                        can hold at least {@code size} bits.
     * @param size how many bits should fit in the returned bitset.
     * @return a bitset of at least {@code size>>5} words. The whole array will be
     *         filled with zero, even beyond bit {@code size}.
     */
    public static int[] init(int[] set, int size) {
        int required = (Math.max(1, size - 1) >> 5) + 1;
        if (set == null || set.length < required) return new int[required];
        Arrays.fill(set, 0);
        return set;
    }

    /** Required {@code long[].length} for storing {@code size} bits. */
    public static int longsFor(int size) { return (size-1 >> 6) +1; }
    /** Required {@code int[].length} for storing {@code size} bits. */
    public static int  intsFor(int size) { return (size-1 >> 5) +1; }

    /* --- --- --- single-bit operations --- --- --- */

    public static boolean   get( int[] set, int i) { return (set[i>>5] &    1  << i) != 0; }
    public static boolean   get(long[] set, int i) { return (set[i>>6] &    1L << i) != 0; }
    public static void      set( int[] set, int i) {         set[i>>5] |=   1  << i; }
    public static void      set(long[] set, int i) {         set[i>>6] |=   1L << i; }
    public static void    clear( int[] set, int i) {         set[i>>5] &= ~(1  << i); }
    public static void    clear(long[] set, int i) {         set[i>>6] &= ~(1L << i); }

    /* --- --- --- range get operations --- --- --- */

    /**
     * Get a 64-bit bitset with bit {@code i} set iff bit {@code begin+i < end} and
     * bit {@code begin+i} is set in {@code set}.
     */
    public static long get(int[] set, int begin, int end) {
        int wIdx = begin>>>5, len = end-begin;
        long subset = set[wIdx] >>> begin;
        for (int i = 32-(begin&31); i < len; i += 32)
            subset |= (long)set[++wIdx] << i;
        return subset &  -1L >>> (64-len);
    }

    /**
     * Get a 64-bit bitset with bit {@code i} set iff bit {@code begin+i < end} and
     * bit {@code begin+i} is set in {@code set}.
     */
    public static long get(long[] set, int begin, int end) {
        int i0 = begin>>6, i1 = end-1>>6;
        long word = set[i0]>>>begin;
        if (i1 != i0)
            word |= set[i1]<<(64-(begin&63));
        return word & -1L >>> (64-(end-begin));
    }

    /* --- --- --- range set/clear operations --- --- --- */

    /**
     * Sets all bits from {@code begin} (inclusive) to {@code end} (exclusive) to zero.
     *
     * @param set the bitset If {@code set.length <= end>>5}, will die horribly
     * @param begin index of the first bit to be set to zero.
     * @param end index of the first bit to NOT be set to zero.
     */
    public static void clear(int[] set, int begin, int end) {
        if (end <= begin)
            return; //no-op
        int w = begin>>5, lastW = (end-1)>>5;
        int firstMask = -1 << begin, lastMask = -1 >>> -end;
        if (w == lastW) {
            set[w] &= ~(firstMask & lastMask);
        } else {
            set[w++] &= ~firstMask;
            while (w < lastW) set[w++] = 0;
            set[lastW] &= ~lastMask;
        }
    }

    /** {@code long[]} version of {@link BS#clear(int[], int, int)} */
    public static void clear(long[] set, int begin, int end) {
        if (end <= begin)
            return; //no-op
        int w = begin>>6, lastW = (end-1)>>6;
        long firstMask = -1L << begin, lastMask = -1L >>> -end;
        if (w == lastW) {
            set[w] &= ~(firstMask & lastMask);
        } else {
            set[w++] &= ~firstMask;
            while (w < lastW) set[w++] = 0;
            set[lastW] &= ~lastMask;
        }
    }

    /**
     * Sets all bits from {@code begin} (inclusive) to {@code end} (exclusive) to one.
     *
     * @param set the bitset If {@code set.length <= end>>5}, will die horribly
     * @param begin index of the first bit to be set to one.
     * @param end index of the first bit to NOT be set to one.
     */
    public static void set(int[] set, int begin, int end) {
        if (end <= begin)
            return; // no-op
        int w = begin>>5, lastW = (end-1)>>5;
        int firstMask = -1 << begin, lastMask = -1 >>> -end;
        if (w == lastW) {
            set[w] |= (firstMask & lastMask);
        } else {
            set[w++] |= firstMask;
            while (w < lastW) set[w++] = -1;
            set[lastW] |= lastMask;
        }
    }

    /** {@code long[]} version of {@link BS#set(int[], int, int)} */
    public static void set(long[] set, int begin, int end) {
        if (end <= begin)
            return; // no-op
        int w = begin>>6, lastW = (end-1)>>6;
        long firstMask = -1L << begin, lastMask = -1L >>> -end;
        if (w == lastW) {
            set[w] |= (firstMask & lastMask);
        } else {
            set[w++] |= firstMask;
            while (w < lastW) set[w++] = -1;
            set[lastW] |= lastMask;
        }
    }

    /* --- --- --- bitwise operations --- --- --- */

    public static void copy(int[] dest, int[] r) {
        int shared = Math.min(dest.length, r.length);
        //noinspection ManualArrayCopy
        for (int i = 0; i < shared; i++) dest[i] = r[i];
    }

    public static void copy(long[] dest, long[] r) {
        int shared = Math.min(dest.length, r.length);
        //noinspection ManualArrayCopy
        for (int i = 0; i < shared; i++) dest[i] = r[i];
    }

    public static void negate(int[] dest, int[] r) {
        int shared = Math.min(dest.length, r.length);
        for (int i = 0; i < shared; i++) dest[i] = ~r[i];
    }

    public static void negate(long[] dest, long[] r) {
        int shared = Math.min(dest.length, r.length);
        for (int i = 0; i < shared; i++) dest[i] = ~r[i];
    }

    public static void andNot(int[] l, int[] r) {
        int shared = Math.min(l.length, r.length);
        for (int i = 0; i < shared; i++) l[i] &= ~r[i];
        for (int i = shared; i < l.length; i++) l[i] = -1;
    }
    public static void andNot(long[] l, long[] r) {
        int shared = Math.min(l.length, r.length);
        for (int i = 0; i < shared; i++) l[i] &= ~r[i];
        for (int i = shared; i < l.length; i++) l[i] = -1;
    }

    public static void and(int[] l, int[] r) {
        int shared = Math.min(l.length, r.length);
        for (int i = 0; i < shared; i++) l[i] &= r[i];
        for (int i = shared; i < l.length; i++) l[i] = 0;
    }
    public static void and(long[] l, long[] r) {
        int shared = Math.min(l.length, r.length);
        for (int i = 0; i < shared; i++) l[i] &= r[i];
        for (int i = shared; i < l.length; i++) l[i] = 0;
    }

    public static void or(int[] l, int[] r) {
        for (int i = 0, shared = Math.min(l.length, r.length); i < shared; i++) l[i] |= r[i];
    }
    public static void or(long[] l, long[] r) {
        for (int i = 0, shared = Math.min(l.length, r.length); i < shared; i++) l[i] |= r[i];
    }

    public static void orAnd(int[] a, int[] b, int[] c) {
        for (int i = 0, shared = Math.min(a.length, Math.min(b.length, c.length)); i < shared; i++)
            a[i] |= b[i] & c[i];
    }
    public static void orAnd(long[] a, long[] b, long[] c) {
        for (int i = 0, shared = Math.min(a.length, Math.min(b.length, c.length)); i < shared; i++)
            a[i] |= b[i] & c[i];
    }

    /* --- --- --- set/unset bit iteration --- --- --- */

    /**
     * First {@code i >= from} where {@code (set[i>>5] & (1 << i)) != 0} or {@code -1}.
     *
     * <p>This is analogous to {@link java.util.BitSet#nextSetBit(int)}</p>
     *
     * @param set the bitset to scan
     * @param from first bit index to check for a set bit.
     * @return The aforementioned {@code i} or -1 if no such {@code i exists}.
     */
    public static int nextSet(int[] set, int from) {
        int wIdx = from>>5, word = wIdx >= set.length ? 0 : set[wIdx] & -1<<from;
        while (word == 0 && ++wIdx < set.length) word = set[wIdx];
        return word == 0 ? -1 : (wIdx<<5) + numberOfTrailingZeros(word);
    }

    /** {@code long} version of {@link BS#nextSet(int[], int)} */
    public static int nextSet(long[] set, int from) {
        int wIdx = from>>6;
        long word = wIdx >= set.length ? 0 : set[wIdx] & -1L<<from;
        while (word == 0 && ++wIdx < set.length) word = set[wIdx];
        return word == 0 ? -1 : (wIdx<<6) + numberOfTrailingZeros(word);
    }

    /**
     * {@code (set.length+1)<<5} or first {@code i >= from} where {@code set[i>>5]&(1<<i) !=0}
     */
    public static int nextSetOrLen(int[] set, int from) {
        int wIdx = from>>5, word = wIdx >= set.length ? 0 : set[wIdx] & -1<<from;
        while (word == 0 && ++wIdx < set.length) word = set[wIdx];
        return word == 0 ? set.length+1<<5 : (wIdx<<5) + numberOfTrailingZeros(word);
    }

    public static int nextSetOrLen(long[] set, int from) {
        int wIdx = from>>6;
        long word = wIdx >= set.length ? 0 : set[wIdx] & -1L<<from;
        while (word == 0 && ++wIdx < set.length) word = set[wIdx];
        return word == 0 ? set.length+1<<6 : (wIdx<<6) + numberOfTrailingZeros(word);
    }

    /**
     * First {@code i >= from} where {@code i >= set.length<<5 || (set[i>>5] & (1 << i)) == 0}.
     *
     * <p>This is analogous to {@link java.util.BitSet#nextClearBit(int)}</p>
     *
     * @param set the bitset to scan
     * @param from first bit index to check for an unset bit.
     * @return The aforementioned {@code i} or {@code set.length <<5} (the bits capacity of
     *         {@code set}) if no such {@code i} exists.
     */
    public static int nextClear(int[] set, int from) {
        int wIdx = from>>5;
        if (wIdx >= set.length) return set.length<<5;
        int word = ~set[wIdx] & (-1 << from);
        while (true) {
            if      (word   !=          0) return (wIdx<<5) + numberOfTrailingZeros(word);
            else if (++wIdx == set.length) return set.length<<5;
            word = ~set[wIdx];
        }
    }

    /** {@code long} version of {@link BS#nextClear(int[], int)} */
    public static int nextClear(long[] set, int from) {
        int wIdx = from>>6;
        if (wIdx >= set.length) return set.length<<6;
        long word = ~set[wIdx] & (-1L << from);
        while (true) {
            if      (word   !=          0) return (wIdx<<6) + numberOfTrailingZeros(word);
            else if (++wIdx == set.length) return set.length<<6;
            word = ~set[wIdx];
        }
    }

    /***
     * Count how many bits are set in {@code set}. Analogous to {@link BitSet#cardinality()}.
     *
     * @param set the bitset
     * @return number of bits set to {@code 1} in the whole {@code set}
     */
    public static int cardinality(int[] set) {
        int card = 0;
        for (int i : set) card += Integer.bitCount(i);
        return card;
    }

    /** {@code long} version of {@link BS#cardinality(int[])}  */
    public static int cardinality(long[] set) {
        int card = 0;
        for (long i : set) card += Long.bitCount(i);
        return card;
    }

    public static int cardinality(int[] set, int begin, int end) {
        int wIdx = begin >> 5, len = end-begin;
        if (len <=  0) return 0;
        if (len <  32) return bitCount(set[wIdx]>>>begin & -1>>>32-len);
        int sum = bitCount(set[wIdx] >>> begin);
        for (len -= 32-(begin&31); len > 32; len -= 32, ++wIdx)
            sum += bitCount(set[wIdx]);
        return sum + (len == 0 ? 0 : bitCount(set[wIdx] & -1>>>32-len));
    }

    public static int cardinality(long[] set, int begin, int end) {
        int wIdx = begin >> 6, len = end-begin;
        if (len <=  0) return 0;
        if (len <  64) return bitCount(set[wIdx]>>>begin & -1L>>>64-len);
        int sum = bitCount(set[wIdx] >>> begin);
        for (len -= 64-(begin&63); len > 64; len -= 64, ++wIdx)
            sum += bitCount(set[wIdx]);
        return sum + (len == 0 ? 0 : bitCount(set[wIdx] & -1L>>>64-len));
    }
}
