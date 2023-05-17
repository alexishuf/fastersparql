package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.store.index.RopeHandlePool;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.Rope.ALPHANUMERIC;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class Splitter {
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
    public static final int MAX_SHARED_ID = 0x00ffffff;

    public SharedSide sharedSide = SharedSide.NONE;
    private PlainRope str = EMPTY;
    private int suffixBegin = 0;
    private final Mode mode;
    private final SegmentRope sharedView = RopeHandlePool.segmentRope();
    private final SegmentRope localView  = RopeHandlePool.segmentRope();
    private TwoSegmentRope tsSharedView, tsLocalView;
    private final ByteRope b64 = new ByteRope(5);

    public Splitter() { this(Mode.LAST); }
    public Splitter(Mode mode) {
        if (mode == null)
            throw new IllegalArgumentException("mode cannot be null");
        this.mode = mode;
    }

    public enum Mode {
        LAST,
        PENULTIMATE,
        PROLONG
    }

    public enum SharedSide {
        PREFIX, SUFFIX, NONE;

        public static final byte PREFIX_CHAR = '.';
        public static final byte SUFFIX_CHAR = '!';

        public byte concatChar() {
            return switch (this) {
                case PREFIX,NONE -> PREFIX_CHAR;
                case SUFFIX      -> SUFFIX_CHAR;
            };
        }
        public static SharedSide fromConcatChar(byte c) {
            return switch (c) {
                case PREFIX_CHAR -> PREFIX;
                case SUFFIX_CHAR -> SUFFIX;
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
        if (id > MAX_SHARED_ID || id < 0)
            throw new IllegalArgumentException("id too big");
        int iId = (int)id;
        byte[] u8 = b64.u8();
        b64.len = 5;
        u8[4] = sharedSide.concatChar();
        u8[0] = BITS_2_BASE64[(iId>>18)&0x3f];
        u8[1] = BITS_2_BASE64[(iId>>12)&0x3f];
        u8[2] = BITS_2_BASE64[(iId>> 6)&0x3f];
        u8[3] = BITS_2_BASE64[ iId     &0x3f];
        return b64;
    }

    public PlainRope shared() {
        return switch (sharedSide) {
            case NONE,PREFIX -> wrap(sharedView, str, 0, suffixBegin);
            case SUFFIX      -> wrap(sharedView, str, suffixBegin, str.len-suffixBegin);
        };
    }

    public PlainRope local() {
        return switch (sharedSide) {
            case NONE   -> str instanceof TwoSegmentRope t && (t.fstLen == 0 || t.sndLen == 0)
                         ? wrap(localView, str, 0, str.len) : str;
            case PREFIX -> wrap(localView, str, suffixBegin, str.len-suffixBegin);
            case SUFFIX -> wrap(localView, str, 0, suffixBegin);
        };
    }

    public Mode mode() { return mode; }

    @Override public String toString() {
        return "{side="+sharedSide+", shared="+shared()+", local="+ local()+"}";
    }

    private PlainRope wrap(SegmentRope wrapper, PlainRope str, int begin, int len) {
        if (str instanceof SegmentRope s) {
            wrapper.wrapSegment(s.segment(), s.offset()+begin, len);
            return wrapper;
        } else {
            TwoSegmentRope t = (TwoSegmentRope) str;
            int fstLen = t.fstLen;
            if (begin + len <= fstLen) {
                wrapper.wrapSegment(t.fst, t.fstOff + begin, len);
                return wrapper;
            } else if (begin >= fstLen) {
                wrapper.wrapSegment(t.snd, t.sndOff + begin - fstLen, len);
                return wrapper;
            } else {
                TwoSegmentRope tsw = wrapper == localView ? tsLocalView : tsSharedView;
                boolean created = tsw == null;
                if (created)
                    tsw = new TwoSegmentRope();
                int taken = fstLen - begin;
                tsw.wrapFirst(t.fst, t.fstOff+begin, taken);
                tsw.wrapSecond(t.snd, t.sndOff+Math.max(0, begin-t.fstLen), len-taken);
                if (created) {
                    if (wrapper == localView) tsLocalView  = tsw;
                    else                      tsSharedView = tsw;
                }
                return tsw;
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
                if (i >= len-1)
                    yield SharedSide.NONE;
                suffixBegin = i;
                yield SharedSide.SUFFIX;
            }
            case '<' -> {
                int i = str.skipUntilLast(0, len, '/', '#');
                if (i >= len)
                    yield SharedSide.NONE;
                switch (mode) {
                    //                  .../path/local>
                    //                          i^       // ^ marks is the new value of i
                    case LAST        -> ++i;
                    case PENULTIMATE -> {
                        //                  ..../22601/name>
                        //                       ^    i      // ^ marks the updated value of i
                        int j = str.skipUntilLast(0, i, '/', '#');
                        i = (j < 8 ? i : j)+1; // do not create http:// and https:// shared strings
                    }
                    case PROLONG -> {
                        // .../TCGA-34-2600-g156>
                        //     i    j              // indexes on first for body entry
                        //          i      j       // before second iteration
                        //                 i     j // after second iteration, stop
                        ++i;
                        for (int j, lst = len-1; (j = str.skip(i, len, ALPHANUMERIC)+1) < lst; )
                            i = j;
                    }
                }
                suffixBegin = i;
                yield SharedSide.PREFIX;
            }
            default -> SharedSide.NONE;
        };
    }
}