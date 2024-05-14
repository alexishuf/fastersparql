package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.model.rope.Rope.ALPHANUMERIC;
import static com.github.alexishuf.fastersparql.util.CSUtils.BASE64_2_BITS;
import static com.github.alexishuf.fastersparql.util.CSUtils.BITS_2_BASE64;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class Splitter extends AbstractOwned<Splitter> {
    public static final int MAX_SHARED_ID = 0x00ffffff;
    public static final int BYTES = 16 /* header */ + 8*4 /* fields */
            + 2*SegmentRopeView.BYTES + 2*TwoSegmentRope.BYTES + MutableRope.BYTES;

    private static final Supplier<Splitter> FAC = new Supplier<>() {
        @Override public Splitter get() {return new Splitter.Concrete().takeOwnership(RECYCLED);}
        @Override public String toString() {return "Splitter.FAC";}
    };
    private static final Alloc<Splitter> ALLOC = new Alloc<>(Splitter.class,
            "Splitter.ALLOC", Alloc.THREADS*32, FAC, BYTES);
    static { Primer.INSTANCE.sched(ALLOC::prime); }

    public SharedSide sharedSide = SharedSide.NONE;
    private PlainRope str = FinalSegmentRope.EMPTY;
    private int suffixBegin = 0;
    private Mode mode = Mode.LAST;
    private final SegmentRopeView sharedView = new SegmentRopeView();
    private final SegmentRopeView localView  = new SegmentRopeView();
    private TwoSegmentRope tsSharedView, tsLocalView;
    private final byte[] b64Bytes = new byte[5];
    private final SegmentRopeView b64 = new SegmentRopeView().wrap(b64Bytes);

    private Splitter() {}

    public static Orphan<Splitter> create(Mode mode) {
        Splitter s = ALLOC.create();
        s.mode = mode;
        return s.releaseOwnership(RECYCLED);
    }

    @Override public @Nullable Splitter recycle(Object currentOwner) {
        internalMarkRecycled(currentOwner);
        if (ALLOC.offer(this) != null)
            internalMarkGarbage(RECYCLED);
        return null;
    }

    private static final class Concrete extends Splitter implements Orphan<Splitter> {
        @Override public Splitter takeOwnership(Object o) {return takeOwnership0(o);}
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
        b64Bytes[4] = sharedSide.concatChar();
        b64Bytes[0] = BITS_2_BASE64[(iId>>18)&0x3f];
        b64Bytes[1] = BITS_2_BASE64[(iId>>12)&0x3f];
        b64Bytes[2] = BITS_2_BASE64[(iId>> 6)&0x3f];
        b64Bytes[3] = BITS_2_BASE64[ iId     &0x3f];
        b64.len     = 5;
        return b64;
    }

    public PlainRope shared() {
        int begin = 0, len = suffixBegin;
        if (sharedSide == SharedSide.SUFFIX) {
            begin = len;
            len = str.len - len;
        }
        return wrap(sharedView, str, begin, len);
    }

    public PlainRope local() {
        int begin = 0, len = suffixBegin;
        SharedSide side = sharedSide;
        if (side == SharedSide.PREFIX) {
            begin = len; len = str.len-len;
        } else if (side == SharedSide.NONE) {
            if (!(str instanceof TwoSegmentRope t) || (t.fstLen != 0 && t.sndLen != 0))
                return str;
            len = str.len;
        }
        return wrap(localView, str, begin, len);
    }

    public Mode mode() { return mode; }
    public void mode(Mode mode) { this.mode = mode; }

    @Override public String toString() {
        return "{side="+sharedSide+", shared="+shared()+", local="+ local()+"}";
    }

    private PlainRope wrap(SegmentRopeView wrapper, PlainRope str, int begin, int len) {
        if (str instanceof SegmentRope s) {
            wrapper.wrap(s.segment, s.utf8, s.offset()+begin, len);
            return wrapper;
        } else {
            TwoSegmentRope t = (TwoSegmentRope) str;
            int fstLen = t.fstLen;
            if (begin + len <= fstLen) {
                wrapper.wrap(t.fst, t.fstU8, t.fstOff + begin, len);
                return wrapper;
            } else if (begin >= fstLen) {
                wrapper.wrap(t.snd, t.sndU8, t.sndOff + begin - fstLen, len);
                return wrapper;
            } else {
                var tsw = wrapper == localView ? tsLocalView : tsSharedView;
                boolean created = tsw == null;
                if (created)
                    tsw = new TwoSegmentRope();
                int taken = fstLen - begin;
                tsw.wrapFirst(t.fst, t.fstU8, t.fstOff+begin, taken);
                tsw.wrapSecond(t.snd, t.sndU8, t.sndOff+Math.max(0, begin-t.fstLen), len-taken);
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
                int i = str.skipUntilLast(0, len, (byte)'"');
                if (i >= len-1)
                    yield SharedSide.NONE;
                suffixBegin = i;
                yield SharedSide.SUFFIX;
            }
            case '<' -> {
                int i = str.skipUntilLast(0, len, (byte)'/', (byte)'#');
                if (i >= len)
                    yield SharedSide.NONE;
                switch (mode) {
                    //                  .../path/local>
                    //                          i^       // ^ marks is the new value of i
                    case LAST        -> ++i;
                    case PENULTIMATE -> {
                        //                  ..../22601/name>
                        //                       ^    i      // ^ marks the updated value of i
                        int j = str.skipUntilLast(0, i, (byte)'/', (byte)'#');
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
