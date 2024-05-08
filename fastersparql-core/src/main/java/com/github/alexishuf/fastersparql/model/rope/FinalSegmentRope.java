package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import static com.github.alexishuf.fastersparql.model.rope.RopeFactory.make;
import static com.github.alexishuf.fastersparql.model.rope.RopeFactory.requiredBytes;

/**
 * A {@link SegmentRope} which:
 *
 * <ol>
 *     <li>Always refer to the same {@link MemorySegment} and {@code byte[]}</li>
 *     <li>The MemorySegment lifecycle is independent of the rope
 *         (collection of the rope will not invalidate the referenced bytes)</li>
 *     <li>Its {@code offset} and {@code len} do not change</li>
 * </ol>
 */
@SuppressWarnings("unused")
public class FinalSegmentRope extends SegmentRope {
    public static final FinalSegmentRope EMPTY     = new FinalSegmentRope(EMPTY_UTF8);
    public static final FinalSegmentRope DQ        = new FinalSegmentRope(new byte[]{'"'});
    public static final FinalSegmentRope GT        = new FinalSegmentRope(new byte[]{'>'});
    public static final FinalSegmentRope DT_MID    = new FinalSegmentRope(new byte[]{'"', '^', '^'});
    public static final FinalSegmentRope DT_MID_LT = new FinalSegmentRope(new byte[]{'"', '^', '^', '<'});

    public FinalSegmentRope(byte[] utf8) {
        super(MemorySegment.ofArray(utf8), utf8, 0, utf8.length);
    }

    public static FinalSegmentRope asFinal(byte @Nullable[] u8) {
        if (u8 == null || u8.length == 0)
            return EMPTY;
        return new FinalSegmentRope(u8);
    }

    /** Create a {@link FinalSegmentRope} wrapping the {@code [offset, offset+len)}
     *  range of {@code utf8} */
    public FinalSegmentRope(byte[] utf8, int offset, int len) {
        super(MemorySegment.ofArray(utf8), utf8, offset, len);
    }

    public static FinalSegmentRope asFinal(byte @Nullable[] utf8, int offset, int len) {
        if (utf8 == null || len == 0) return EMPTY;
        return new FinalSegmentRope(utf8, offset, len);
    }

    /**
     * Create a {@link FinalSegmentRope} wrapping the whole segment
     * @throws IllegalArgumentException if {@link MemorySegment#byteSize()} is larger
     *                                  than {@link Integer#MAX_VALUE}
     */
    public FinalSegmentRope(MemorySegment segment) {
        super(segment, null, 0, asIntLen(segment.byteSize()));
    }

    public static FinalSegmentRope asFinal(@Nullable MemorySegment segment) {
        int len = segment == null ? 0 : asIntLen(segment.byteSize());
        return len == 0 ? EMPTY : new FinalSegmentRope(segment, null, 0, len);
    }

    /** Create a {@link FinalSegmentRope} wrapping the {@code offset, offset+len)} range
     *  within {@code segment} */
    public FinalSegmentRope(MemorySegment segment, long offset, int len) {
        super(segment, null, offset , len);
    }

    public static FinalSegmentRope asFinal(@Nullable MemorySegment segment, long offset, int len) {
        return segment == null || len == 0 ? EMPTY : new FinalSegmentRope(segment, offset, len);
    }

    /** Equivalent to {@link FinalSegmentRope#FinalSegmentRope(MemorySegment)}, with
     *  {@code utf8 == segment.heapBase().orElse(null} */
    public FinalSegmentRope(MemorySegment segment, byte @Nullable[] utf8) {
        super(segment, utf8, 0, asIntLen(segment.byteSize()));
    }

    public static FinalSegmentRope asFinal(@Nullable MemorySegment segment, byte @Nullable[] utf8) {
        int len = segment == null ? 0 : asIntLen(segment.byteSize());
        return len == 0 ? EMPTY : new FinalSegmentRope(segment, utf8, 0, len);
    }

    /** Equivalent to {@link FinalSegmentRope#FinalSegmentRope(MemorySegment, long, int)}, with
     *  {@code utf8 == segment.heapBase().orElse(null} */
    public FinalSegmentRope(MemorySegment segment, byte @Nullable[] utf8, long offset, int len) {
        super(segment, utf8, offset , len);
    }

    public static  FinalSegmentRope asFinal(@Nullable MemorySegment segment, byte @Nullable[] utf8,
                                            long offset, int len) {
        return segment == null || len == 0 ? EMPTY
                : new FinalSegmentRope(segment, utf8, offset, len);
    }

    public FinalSegmentRope(ByteBuffer bb) {
        super(MemorySegment.ofBuffer(bb), null, 0, bb.remaining());
    }

    public static FinalSegmentRope asFinal(ByteBuffer bb) {
        return bb == null || bb.remaining() == 0 ? EMPTY : new FinalSegmentRope(bb);
    }

    public static FinalSegmentRope asFinal(@Nullable StringBuilder other) {
        return make(requiredBytes(other)).add(other).take();
    }
    public static FinalSegmentRope asFinal(@Nullable StringBuffer other) {
        return make(requiredBytes(other)).add(other).take();
    }
    public static FinalSegmentRope asFinal(@Nullable ByteArrayOutputStream os) {
        return os == null ? EMPTY : asFinal(os.toByteArray());
    }
    public static FinalSegmentRope asFinal(@Nullable String other) {
        return make(requiredBytes(other)).add(other).take();
    }
    public static FinalSegmentRope asFinal(@Nullable Object other) {
        return switch (other) {
            case null -> EMPTY;
            case FinalSegmentRope f -> f;
            case byte[] u8 -> asFinal(u8);
            default -> {
                CharSequence cs = other instanceof CharSequence s ? s : other.toString();
                yield make(requiredBytes(cs)).add(cs).take();
            }
        };
    }
    public static FinalSegmentRope asFinal(@Nullable CharSequence other) {
        if (other == null) return EMPTY;
        if (other instanceof FinalSegmentRope f) return f;
        return make(requiredBytes(other)).add(other).take();
    }
    public static FinalSegmentRope asFinal(@Nullable SegmentRope other) {
        if (other == null) return EMPTY;
        if (other instanceof FinalSegmentRope f) return f;
        return make(other.len).add(other).take();
    }
    public static FinalSegmentRope asFinal(@Nullable SegmentRopeView other) {
        if (other == null) return EMPTY;
        return make(other.len).add(other).take();
    }
    public static FinalSegmentRope asFinal(@Nullable MutableRope other) {
        if (other == null || other.len == 0) return EMPTY;
        return make(other.len).add(other).take();
    }
    public static FinalSegmentRope asFinal(@Nullable TwoSegmentRope other) {
        if (other == null) return EMPTY;
        return make(other.len).add(other).take();
    }
    public static FinalSegmentRope asFinal(@Nullable Term other) {
        if (other == null) return EMPTY;
        return make(other.len).add(other).take();
    }
    public static FinalSegmentRope asFinal(StringBuffer other, int begin, int end) {
        if (end == begin) return EMPTY;
        return make(requiredBytes(other, begin, end)).add(other, begin, end).take();
    }
    public static FinalSegmentRope asFinal(StringBuilder other, int begin, int end) {
        if (end == begin) return EMPTY;
        return make(requiredBytes(other, begin, end)).add(other, begin, end).take();
    }
    public static FinalSegmentRope asFinal(String other, int begin, int end) {
        if (end == begin) return EMPTY;
        return make(requiredBytes(other, begin, end)).add(other, begin, end).take();
    }
    public static FinalSegmentRope asFinal(CharSequence other, int begin, int end) {
        if (end == begin) return EMPTY;
        if (begin == 0 && other instanceof FinalSegmentRope f && end == f.len) return f;
        return make(requiredBytes(other, begin, end)).add(other, begin, end).take();
    }
    public static FinalSegmentRope asFinal(Rope other, int begin, int end) {
        if (end == begin) return EMPTY;
        if (begin == 0 && other instanceof FinalSegmentRope f && end == f.len) return f;
        return make(requiredBytes(other, begin, end)).add(other, begin, end).take();
    }
    public static FinalSegmentRope asFinal(SegmentRope other, int begin, int end) {
        if (end == begin) return EMPTY;
        if (begin == 0 && other instanceof FinalSegmentRope f && end == f.len) return f;
        return make(requiredBytes(other, begin, end)).add(other, begin, end).take();
    }
    public static FinalSegmentRope asFinal(SegmentRopeView other, int begin, int end) {
        if (end == begin) return EMPTY;
        return make(requiredBytes(other, begin, end)).add(other, begin, end).take();
    }
    public static FinalSegmentRope asFinal(MutableRope other, int begin, int end) {
        if (end == begin) return EMPTY;
        return make(requiredBytes(other, begin, end)).add(other, begin, end).take();
    }
    public static FinalSegmentRope asFinal(TwoSegmentRope other, int begin, int end) {
        if (end == begin) return EMPTY;
        return make(requiredBytes(other, begin, end)).add(other, begin, end).take();
    }
    public static FinalSegmentRope asFinal(Term other, int begin, int end) {
        if (end == begin) return EMPTY;
        return make(requiredBytes(other, begin, end)).add(other, begin, end).take();
    }

    /** Create a {@link FinalSegmentRope} that wraps the same segment range as {@code view} */
    public static FinalSegmentRope asFinalByReference(@Nullable SegmentRope other) {
        if (other == null || other.len == 0)
            return EMPTY;
        if (other instanceof FinalSegmentRope f)
            return f;
        if (other instanceof MutableRope m)
            return asFinal(m);
        return new FinalSegmentRope(other.segment, other.utf8, other.offset, other.len);
    }
    /** Create a {@link FinalSegmentRope} that wraps the same segment range as {@code view} */
    public static FinalSegmentRope asFinalByReference(@Nullable SegmentRopeView view) {
        if (view == null || view.len == 0)
            return EMPTY;
        return new FinalSegmentRope(view.segment, view.utf8, view.offset, view.len);
    }
}
