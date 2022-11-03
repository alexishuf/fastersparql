package com.github.alexishuf.fastersparql.client.util.bind;

import java.util.Arrays;
import java.util.BitSet;

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
        int wIdx = from>>5;
        if (wIdx >= set.length) return -1;
        int word = set[wIdx] & (-1 << from);
        if ((word & (1 << from)) != 0) return from;
        while (true) {
            if      (word   !=          0) return (wIdx<<5) + numberOfTrailingZeros(word);
            else if (++wIdx == set.length) return -1;
            word = set[wIdx];
        }
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
}
