package com.github.alexishuf.fastersparql.model.rope;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static jdk.incubator.vector.ByteVector.fromArray;
import static jdk.incubator.vector.ByteVector.fromMemorySegment;

public class RopeSupport {
    private static final VectorSpecies<Byte> B_SP = ByteVector.SPECIES_PREFERRED;

    public static boolean arraysEqual(byte[] l, byte[] r) {
        if (l.length != r.length) return false;
        int i = 0;
        for (int e = B_SP.loopBound(l.length); i < e; e += B_SP.length()) {
            if (!fromArray(B_SP, l, i).eq(fromArray(B_SP, r, i)).allTrue()) return false;
        }
        while (i < l.length && l[i] == r[i]) ++i;
        return i == l.length;
    }

    public static boolean rangesEqual(byte[] l, int lOff, byte[] r, int rOff, int len) {
        int lEnd = lOff+len;
        for (int s = B_SP.length(), e = B_SP.loopBound(len); lOff < e ; lOff += s, rOff += s) {
            if (!fromArray(B_SP, l, lOff).eq(fromArray(B_SP, r, rOff)).allTrue()) return false;
        }
        for (; lOff < lEnd && l[lOff] == r[rOff]; ++lOff) rOff++;
        return lOff == lEnd;
    }

    public static boolean rangesEqual(MemorySegment left, int lOff, MemorySegment right, int rOff, int len) {
        int step = B_SP.length(), lEnd = lOff+len;
        if (lEnd > left.byteSize() || rOff+len > right.byteSize())
            return false;
        for (int e = lOff+B_SP.loopBound(len); lOff < e; lOff += step, rOff += step) {
            ByteVector lv = fromMemorySegment(B_SP, left, lOff, LITTLE_ENDIAN);
            if (!lv.eq(fromMemorySegment(B_SP, right, rOff, LITTLE_ENDIAN)).allTrue()) return false;
        }
        while (lOff < lEnd && left.get(JAVA_BYTE, lOff) == right.get(JAVA_BYTE, rOff++)) lOff++;
        return lOff == lEnd;
    }

    public static int hash(byte[] utf8, int offset, int len) {
        int h = 0, end = offset+len;
        while (offset < end)
            h = 31*h + (0xff&utf8[offset++]);
        return h;
    }

    public static int compareTo(byte[] l, byte[] r) {
        int cmp = 0;
        for (int i = 0, e = Math.min(l.length, r.length); cmp == 0 && i < e; i++)
            cmp = l[i] - r[i];
        return cmp == 0 ? l.length - r.length : cmp;
    }

    public static int compareTo(byte[] l, Rope r, int rBegin, int rEnd) {
        int lBegin = 0, diff = 0;
        while (lBegin < l.length && rBegin < rEnd && diff == 0)
            diff = l[lBegin++] - r.get(rBegin++);
        return diff == 0 ? (l.length-lBegin) - (rEnd-rBegin) : diff;
    }

//    private static final ByteVector ZERO = ByteVector.zero(B_SP);
//    private static final ByteVector ONE = ByteVector.broadcast(B_SP, 1);
//    private static final ByteVector THREE = ByteVector.broadcast(B_SP, 3);
//    public int vecSkip(int begin, int end, byte[] alphabet) {
//        ByteVector bitsets = fromArray(B_SP, alphabet, 0);
//        for (int b = begin + B_SP.loopBound(end-begin); begin < b; begin += B_SP.length()) {
//            ByteVector chars = fromArray(B_SP, utf8, begin);
//            VectorMask<Byte> matchingChars = bitsets
//                    .rearrange(chars.lanewise(VectorOperators.LSHR, THREE).toShuffle())
//                    .and(ONE.lanewise(VectorOperators.LSHL, chars))
//                    .eq(B_SP.zero());
//            byte candidate = IOTA.reduceLanes(MIN, matchingChars);
//            if (candidate != Byte.MAX_VALUE)
//                return begin+candidate;
//        }
//        for (; begin < end; ++begin) {
//            int c = utf8[begin]&0xff;
//            if ((alphabet[c >> 3] & (1 << (c & 7))) == 0)
//                return begin; // c is not in alphabet
//        }
//        return end;
//    }

    public static int skip(byte[] utf8, int begin, int end, int[] alphabet) {
        boolean stopOnNonAscii = (alphabet[3]&0x80000000) == 0;
        for (int i = begin; i < end; ++i) {
            byte c = utf8[i];
            if (c >= 0) { // c is ASCII
                if ((alphabet[c >> 5] & (1 << c)) == 0)
                    return i; // c is not in alphabet
            } else if (stopOnNonAscii) {
                return i; // non-ASCII  not allowed by alphabet
            }
        }
        return end;
    }

    public static int reverseSkip(byte[] utf8, int begin, int end, int[] alphabet) {
        boolean stopOnNonAscii = (alphabet[3]&0x80000000) == 0;
        for (int i = end-1; i >= begin; i--) {
            byte c = utf8[i];
            if (c >= 0) { // c is ASCII
                if ((alphabet[c >> 5] & (1 << c)) == 0)
                    return i; // c is not in alphabet
            } else if (stopOnNonAscii) {
                return i; // non-ASCII  not allowed by alphabet
            }
        }
        return begin;
    }
}
