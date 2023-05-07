package com.github.alexishuf.fastersparql.model.rope;

/**
 * Parent class for {@link SegmentRope} and {@link TwoSegmentRope}. This class exists to
 * help C2 de-virtualize calls by constraining the implementations count.
 */
public abstract class PlainRope extends Rope {
    public PlainRope(int len) { super(len); }

    /** Equivalent to {@link Rope#compareTo(Rope)} but helps the JIT skip some instanceof tests. */
    public final int compareTo(PlainRope o) {
        return o instanceof SegmentRope s ? compareTo(s) : compareTo((TwoSegmentRope)o);
    }
    /** Equivalent to {@link Rope#compareTo(Rope)} but helps the JIT skip some instanceof tests. */
    public abstract int compareTo(SegmentRope o);
    /** Equivalent to {@link Rope#compareTo(Rope)} but helps the JIT skip some instanceof tests. */
    public abstract int compareTo(TwoSegmentRope o);

    /** Equivalent to {@link Rope#compareTo(Rope)} with {@code o.sub(begin, end)} */
    public abstract int compareTo(PlainRope o, int begin, int end);
    /** Equivalent to {@link Rope#compareTo(Rope)} with {@code o.sub(begin, end)} */
    public abstract int compareTo(SegmentRope o, int begin, int end);
    /** Equivalent to {@link Rope#compareTo(Rope)} with {@code o.sub(begin, end)} */
    public abstract int compareTo(TwoSegmentRope o, int begin, int end);
}
