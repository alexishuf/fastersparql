package com.github.alexishuf.fastersparql.model.rope;

/**
 * Parent class for {@link SegmentRope} and {@link TwoSegmentRope}. This class exists to
 * help C2 de-virtualize calls by constraining the implementations count.
 */
public abstract class PlainRope extends Rope {
    public PlainRope(int len) { super(len); }
}
