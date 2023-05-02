package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class StringSplitStrategy {
    private static final byte[] BITS_2_BASE64 = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
            'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
            'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
            'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
            'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', '+', '/'
    };

    private static final byte[] BASE64_2_BITS;
    static {
        byte[] char2value = new byte[128];
        Arrays.fill(char2value, (byte) -1);
        for (int value = 0; value < BITS_2_BASE64.length; value++)
            char2value[BITS_2_BASE64[value]] = (byte) value;
        BASE64_2_BITS = char2value;
    }
    public static final int MIN_SHARED_LEN = 6;
    public static final int MAX_SHARED_ID = 0x00ffffff;

    public SharedSide sharedSide = SharedSide.NONE;
    private PlainRope str = EMPTY;
    private int suffixBegin = 0;
    private final SegmentRope sharedView = RopeHandlePool.segmentRope();
    private final SegmentRope localView  = RopeHandlePool.segmentRope();
    private ByteRope copy = null;
    private final ByteRope b64 = new ByteRope(5);

    public enum SharedSide {
        PREFIX, SUFFIX, NONE;

        public byte concatChar() {
            return switch (this) {
                case PREFIX,NONE -> '.';
                case SUFFIX      -> '!';
            };
        }
        public static SharedSide fromConcatChar(byte c) {
            return switch (c) {
                case '.' -> PREFIX;
                case '!' -> SUFFIX;
                default -> throw new IllegalArgumentException("Only ! and . are allowed");
            };
        }
    }

    public static int decode(MemorySegment segment, long pos) {
        byte v0 = BASE64_2_BITS[segment.get(JAVA_BYTE, pos)];
        byte v1 = BASE64_2_BITS[segment.get(JAVA_BYTE, pos+1)];
        byte v2 = BASE64_2_BITS[segment.get(JAVA_BYTE, pos+2)];
        byte v3 = BASE64_2_BITS[segment.get(JAVA_BYTE, pos+3)];
        if (v0 < 0 || v1 < 0 || v2 < 0 | v3 < 0)
            throw new IllegalArgumentException("Some bytes are not in the base64 alphabet");
        return (v0<<18) | (v1<<12) | (v2<<6) | v3;
    }


    public SegmentRope b64(long id) {
        int iId = id > MAX_SHARED_ID || id < 0 ? (int)Dict.EMPTY_ID : (int)id;
        byte[] u8 = b64.u8();
        b64.len = 5;
        u8[4] = sharedSide.concatChar();
        u8[0] = BITS_2_BASE64[(iId>>18)&0x3f];
        u8[1] = BITS_2_BASE64[(iId>>12)&0x3f];
        u8[2] = BITS_2_BASE64[(iId>> 6)&0x3f];
        u8[3] = BITS_2_BASE64[ iId     &0x3f];
        return b64;
    }

    public SegmentRope shared() {
        switch (sharedSide) {
            case NONE,PREFIX -> wrap(sharedView, str, 0, suffixBegin);
            case SUFFIX      -> wrap(sharedView, str, suffixBegin, str.len-suffixBegin);
        }
        return sharedView;
    }

    public SegmentRope local() {
        switch (sharedSide) {
            case PREFIX,NONE -> wrap(localView, str, suffixBegin, str.len-suffixBegin);
            case SUFFIX      -> wrap(localView, str, 0, suffixBegin);
        }
        return localView;
    }

    public PlainRope localOrWhole() {
        return sharedSide == SharedSide.NONE ? str : local();
    }

    public SegmentRope stealHandle(@Nullable PlainRope except) {
        return localView == except ? sharedView : localView;
    }

    @Override public String toString() {
        return "{side="+sharedSide+", shared="+shared()+", local="+ local()+"}";
    }

    private void wrap(SegmentRope wrapper, PlainRope str, int begin, int len) {
        if (str instanceof SegmentRope s) {
            wrapper.wrapSegment(s.segment(), s.offset()+begin, len);
        } else {
            TwoSegmentRope t = (TwoSegmentRope) str;
            int fstLen = t.fstLen;
            if (begin + len <= fstLen) {
                wrapper.wrapSegment(t.fst, t.fstOff + begin, len);
            } else if (begin > fstLen) {
                wrapper.wrapSegment(t.snd, t.sndOff + begin - fstLen, len);
            } else{
                ByteRope copy = this.copy;
                if (copy == null)
                    this.copy = copy = new ByteRope((len + 32) & ~31);
                copy.clear().append(str, begin, begin + len);
                wrapper.wrap(copy);
            }
        }
    }

    public SharedSide split(PlainRope str) {
        int len  = str.len;
        this.str = str;
        suffixBegin = 0;
        return sharedSide = switch (len == 0 ? 0 : str.get(0)) {
            case '"' -> {
                int i = str.skipUntilLast(0, len, '"');
                if (len-i < MIN_SHARED_LEN)
                    yield SharedSide.NONE;
                suffixBegin = i;
                yield SharedSide.SUFFIX;
            }
            case '<' -> {
                int i = str.skipUntilLast(0, len, '/', '#');
                if (i >= len)
                    yield SharedSide.NONE;
                suffixBegin = i+1;
                yield SharedSide.PREFIX;
            }
            default -> SharedSide.NONE;
        };
    }
}
