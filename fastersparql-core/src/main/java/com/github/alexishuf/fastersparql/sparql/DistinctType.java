package com.github.alexishuf.fastersparql.sparql;

import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;

public enum DistinctType {
    WEAK,
    REDUCED,
    STRONG;

    private static final FinalSegmentRope DISTINCT_ROPE = FinalSegmentRope.asFinal("DISTINCT");
    private static final FinalSegmentRope REDUCED_ROPE = FinalSegmentRope.asFinal("REDUCED");
    private static final FinalSegmentRope WEAK_ROPE = FinalSegmentRope.asFinal("PRUNED");

    public FinalSegmentRope sparql() {
        return switch (this) {
            case STRONG  -> DISTINCT_ROPE;
            case REDUCED -> REDUCED_ROPE;
            case WEAK    -> WEAK_ROPE;
        };
    }

    public static int compareTo(DistinctType left, DistinctType right) {
        if (left  == right) return  0;
        if (left  ==  null) return -1;
        if (right ==  null) return  1;
        return left.compareTo(right);
    }
}
