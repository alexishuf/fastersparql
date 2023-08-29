package com.github.alexishuf.fastersparql.model.rope;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Parent class for {@link SegmentRope} and {@link TwoSegmentRope}. This class exists to
 * help C2 de-virtualize calls by constraining the implementations count.
 */
public abstract class PlainRope extends Rope {
    public PlainRope(int len) { super(len); }

    public final int compareTo(@NonNull Rope o) {
        if      (o instanceof SegmentRope    s) return compareTo(s);
        else if (o instanceof TwoSegmentRope t) return compareTo(t);
        else                                    return super.compareTo(o);
    }

    /** Equivalent to {@link Rope#compareTo(Rope)} but helps the JIT skip some instanceof tests. */
    public final int compareTo(PlainRope o) {
        return o instanceof SegmentRope s ? compareTo(s) : compareTo((TwoSegmentRope)o);
    }
    /** Equivalent to {@link Rope#compareTo(Rope)} but helps the JIT skip some instanceof tests. */
    public abstract int compareTo(SegmentRope o);
    /** Equivalent to {@link Rope#compareTo(Rope)} but helps the JIT skip some instanceof tests. */
    public abstract int compareTo(TwoSegmentRope o);

    @SuppressWarnings("unused") @Override public final int compareTo(Rope o, int begin, int end) {
        if      (o instanceof SegmentRope    s) return compareTo(s, begin, end);
        else if (o instanceof TwoSegmentRope t) return compareTo(t, begin, end);
        else                                    return super.compareTo(o, begin, end);
    }

    /** Equivalent to {@link Rope#compareTo(Rope)} with {@code o.sub(begin, end)} */
    @SuppressWarnings("unused") public final int compareTo(PlainRope o, int begin, int end) {
        return o instanceof SegmentRope s ? compareTo(s, begin, end)
                                          : compareTo((TwoSegmentRope)o, begin, end);
    }
    /** Equivalent to {@link Rope#compareTo(Rope)} with {@code o.sub(begin, end)} */
    public abstract int compareTo(SegmentRope o, int begin, int end);
    /** Equivalent to {@link Rope#compareTo(Rope)} with {@code o.sub(begin, end)} */
    public abstract int compareTo(TwoSegmentRope o, int begin, int end);
}
