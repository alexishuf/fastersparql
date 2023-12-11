package com.github.alexishuf.fastersparql.sparql;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;

import static java.nio.charset.StandardCharsets.UTF_8;

public enum DistinctType {
    WEAK,
    REDUCED,
    STRONG;

    private static final ByteRope DISTINCT_ROPE = new ByteRope("DISTINCT".getBytes(UTF_8));
    private static final ByteRope REDUCED_ROPE = new ByteRope("REDUCED".getBytes(UTF_8));
    private static final ByteRope WEAK_ROPE = new ByteRope("PRUNED".getBytes(UTF_8));

    public ByteRope sparql() {
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
