package com.github.alexishuf.fastersparql.store.index;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout.OfInt;
import java.lang.foreign.ValueLayout.OfLong;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public enum ValueWidth {
    LE4B,
    LE8B;

    private static final long INT_MASK = 0x00000000ffffffffL;
    private static final OfInt LE4B_LAYOUT =
            JAVA_INT.order() == LITTLE_ENDIAN ? JAVA_INT : JAVA_INT.withOrder(LITTLE_ENDIAN);
    private static final OfLong LE8B_LAYOUT =
            JAVA_LONG.order() == LITTLE_ENDIAN ? JAVA_LONG : JAVA_LONG.withOrder(LITTLE_ENDIAN);

    public int bytes() { return this == LE4B ? 4 : 8; }

    /**
     * Read the value at the {@code address}-th byte of {@code s}.
     * @param s {@link MemorySegment} from where to read the value of this {@link ValueWidth}
     * @param address the offset in bytes into {@code s} where the value is stored.
     * @return A {@code long} with the value (higher bytes will be zero if this
     *         {@link ValueWidth} has less than 8 bytes).
     */
    public final long read(MemorySegment s, long address)  {
        return this == LE4B ? s.get(LE4B_LAYOUT, address) & INT_MASK
                            : s.get(LE8B_LAYOUT, address);
    }

    /**
     * Reads the value of this {@link ValueWidth} after another value of same width that
     * starts at byte {@code address}.
     *
     * @param s {@link MemorySegment} from where the value shall be read.
     * @param address bytes offset into {@code s} where the value <strong>preceding</strong>
     *                the value to be read is stored.
     * @return A {@code long} with the value after the value at {@code address}. If this
     *         {@link ValueWidth} has less than 8 bytes, higher bytes will be zero.
     */
    public final long readNext(MemorySegment s, long address) {
        return this == LE4B ? s.get(LE4B_LAYOUT, address+4) & INT_MASK
                            : s.get(LE8B_LAYOUT, address+8);
    }

    /**
     * Get the offset corresponding to {@code index} occurrences of values with this width
     *
     * @param index a zero-based index into a sequence of values all having this {@link ValueWidth}
     * @return {@code index} multiplied by the number of bytes of this {@link ValueWidth}.
     */
    public final long idx2addr(long index) {
        return index << (this == LE4B ? 2 : 3);
    }

    /**
     * Get the index of the item that starts at byte offset in sequence of values that all
     * have the same {@link ValueWidth}. Note that this will <strong>silently align offset</strong>
     * by rounding it down to the value width.
     *
     * @param offset a bytes offset into a sequence of values of the same {@link ValueWidth}
     * @return the zero-based index into the sequence of values that corresponds to the
     *         given {@code offset}.
     */
    public final long addr2idx(long offset) {
        return offset >> (this == LE4B ? 2 : 3);
    }

    /**
     * Read the {@code index}-th element in a sequence of values with this {@link ValueWidth} that
     * starts at byte {@code base}.
     *
     * @param s The {@link MemorySegment} from where the value will be read.
     * @param base bytes offset into {@code s} where a list of values of this {@link ValueWidth}
     *             start
     * @param index zero-based index of the item to be read within the list.
     * @return A {@code long} with the value read. If this {@link ValueWidth} is less than 8
     *         bytes wide the higher bytes of the {@code long} will be zero.
     */
    public final long readIdx(MemorySegment s, long base, long index) {
        if (this == LE4B)
            return INT_MASK&s.get(LE4B_LAYOUT, base + (index << 2));
        return s.get(LE8B_LAYOUT, base + (index << 3));
    }

    /**
     * Get {@link #LE4B} iff bit {@code bitIndex} of {@code value} is set.
     *
     * @param value a long value containing a flag bit
     * @param bitIndex the zero-based index of the flag bit
     * @return if the flag bit is set, {@link #LE4B}, else {@link #LE8B}.
     */
    public static ValueWidth le4If(long value, int bitIndex) {
        return ((int)(value >>> bitIndex)&1) == 0 ? LE8B : LE4B;
    }
}
