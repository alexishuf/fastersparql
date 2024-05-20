package com.github.alexishuf.fastersparql.model.rope;

import static java.lang.Character.*;

public class RopeEncoder {
    @SuppressWarnings("UnnecessaryUnicodeEscape")
    public static int char2utf8(char c, byte[] dst, int dstPos) {
        if (c < '\u0080') {
            dst[dstPos] = (byte)c;
            return dstPos+1;
        } else {
            byte last = (byte)(0x80|(c&0x3f)), beforeLast = (byte)(c>>6);
            if (c < '\u0800') {
                dst[dstPos  ] = (byte)(0xc0|beforeLast);
                dst[dstPos+1] = last;
                return dstPos+2;
            } else {
                if (c < Character.MIN_SURROGATE || c > Character.MAX_SURROGATE) {
                    dst[dstPos  ] = (byte)(0xe0|(c>>12));
                    dst[dstPos+1] = (byte)(0x80|beforeLast);
                    dst[dstPos+2] = last;
                } else { // write REPLACEMENT CHAR: � (U+FFFD)
                    dst[dstPos  ] = (byte)0xEF;
                    dst[dstPos+1] = (byte)0xBF;
                    dst[dstPos+2] = (byte)0xBD;
                }
                return dstPos+3;
            }
        }
    }

    public static int codePoint2utf8(int code, byte[] dst, int dstPos) {
        /* 4-bytes UTF-8 encoding
         *
         * Input (binary): 0babcdefghijklmnopqrstuvwxyzABCDEFG
         * +------------+-----------+-----------------+----------+----------+----------+
         * | first code | last code |          byte 0 |   byte 2 |   byte 3 |   byte 4 |
         * | 0x00000    | 0x00007f  | (0x00) 0ABCDEFG |          |          |          |
         * | 0x00080    | 0x0007ff  | (0xc0) 110wxyzA | 10BCDEFG |          |          |
         * | 0x00800    | 0x00ffff  | (0xe0) 1110rstu | 10vwxyzA | 10BCDEFG |          |
         * | 0x10000    | 0x10ffff  | (0xf0) 11110mno | 10pqrstu | 10vwxyzA | 10BCDEFG |
         * +------------+-----------+-----------------+----------+----------+----------+
         */
        if (code < 0x80) {
            dst[dstPos] = (byte)code;
            return dstPos+1;
        } else {
            int last = 0x80|(code&0x3f), beforeLast = (code>>6)&0x3f;
            if (code < 0x800) {
                dst[dstPos  ] = (byte)(0xc0|beforeLast);
                dst[dstPos+1] = (byte)last;
                return dstPos+2;
            } else if (code < 0x10000) {
                dst[dstPos  ] = (byte)(0xe0|(code>>12));
                dst[dstPos+1] = (byte)(0x80|beforeLast);
                dst[dstPos+2] = (byte)last;
                return dstPos+3;
            } else if (code < 0x10ffff) {
                dst[dstPos  ] = (byte)(0xf0|(code>>18));
                dst[dstPos+1] = (byte)(0x80|((code>>12)&0x3f));
                dst[dstPos+2] = (byte)(0x80|beforeLast);
                dst[dstPos+3] = (byte)last;
                return dstPos+4;
            } else { // write REPLACEMENT CHAR: � (U+FFFD)
                dst[dstPos  ] = (byte)0xEF;
                dst[dstPos+1] = (byte)0xBF;
                dst[dstPos+2] = (byte)0xBD;
                return dstPos+3;
            }
        }
    }

    public interface ByteAppend<T> {
        void append(T dst, byte utf8Byte);
    }
    public static final ByteSinkAppender BYTE_SINK_APPENDER = new ByteSinkAppender();
    public static final MutableRopeAppender MUTABLE_ROPE_APPENDER = new MutableRopeAppender();
    public static final class ByteSinkAppender implements ByteAppend<ByteSink<?, ?>> {
        private ByteSinkAppender() {}
        @Override public void append(ByteSink<?, ?> dst, byte utf8Byte) {dst.append(utf8Byte);}
    }
    public static final class MutableRopeAppender implements ByteAppend<MutableRope> {
        private MutableRopeAppender() {}
        @Override public void append(MutableRope dst, byte utf8Byte) {dst.append(utf8Byte);}
    }

    public static <T> void codePoint2utf8(int code,  ByteAppend<T> append, T dst) {
        if (code < 0x80) {
            append.append(dst, (byte)code);
        } else {
            int last = 0x80|(code&0x3f), beforeLast = (code>>6)&0x3f;
            if (code < 0x800) {
                append.append(dst, (byte)(0xc0|beforeLast));
                append.append(dst, (byte)last);
            } else if (code < 0x10000) {
                append.append(dst, (byte)(0xe0|(code>>12)));
                append.append(dst, (byte)(0x80|beforeLast));
                append.append(dst, (byte)last);
            } else if (code < 0x10ffff) {
                append.append(dst, (byte)(0xf0|(code>>18)));
                append.append(dst, (byte)(0x80|((code>>12)&0x3f)));
                append.append(dst, (byte)(0x80|beforeLast));
                append.append(dst, (byte)last);
            } else { // write REPLACEMENT CHAR: � (U+FFFD)
                append.append(dst, (byte)0xEF);
                append.append(dst, (byte)0xBF);
                append.append(dst, (byte)0xBD);
            }
        }
    }

    public static int charSequence2utf8(CharSequence cs, int begin, int end,
                                        byte[] dst, int dstPos) {
        char c, c1;
        for (int i = begin, u; i < end; i++) {
            c = cs.charAt(i);
            if (c < MIN_SURROGATE || c > MAX_SURROGATE) {
                u = c;
            } else if (c <= MAX_HIGH_SURROGATE && i+1 < end && isLowSurrogate(c1=cs.charAt(i+1))) {
                u = Character.toCodePoint(c, c1);
                ++i;
            } else {
                u = 0xfffd; // invalid surrogate pair, REPLACEMENT CHAR
            }
            dstPos = codePoint2utf8(u, dst, dstPos);
        }
        return dstPos;
    }

    public static <T> void charSequence2utf8(CharSequence cs, int begin, int end,
                                             ByteAppend<T> append, T dst) {
        char c, c1;
        for (int i = begin, u; i < end; i++) {
            c = cs.charAt(i);
            if (c < MIN_SURROGATE || c > MAX_SURROGATE) {
                u = c;
            } else if (c <= MAX_HIGH_SURROGATE && i+1 < end && isLowSurrogate(c1=cs.charAt(i+1))) {
                u = Character.toCodePoint(c, c1);
                ++i;
            } else {
                u = 0xfffd; // invalid surrogate pair, REPLACEMENT CHAR
            }
            codePoint2utf8(u, append, dst);
        }
    }
    public static <T> void charSequence2utf8(char[] cs, int begin, int end,
                                             ByteAppend<T> append, T dst) {
        char c, c1;
        for (int i = begin, u; i < end; i++) {
            c = cs[i];
            if (c < MIN_SURROGATE || c > MAX_SURROGATE) {
                u = c;
            } else if (c <= MAX_HIGH_SURROGATE && i+1 < end && isLowSurrogate(c1=cs[i+1])) {
                u = Character.toCodePoint(c, c1);
                ++i;
            } else {
                u = 0xfffd; // invalid surrogate pair, REPLACEMENT CHAR
            }
            codePoint2utf8(u, append, dst);
        }
    }
}
