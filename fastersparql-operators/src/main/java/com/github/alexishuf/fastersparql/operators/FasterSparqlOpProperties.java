package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.util.FasterSparqlProperties;
import org.checkerframework.checker.index.qual.Positive;

public class FasterSparqlOpProperties extends FasterSparqlProperties {

    /* --- --- --- property names --- --- --- */
    public static final String OP_DISTINCT_WINDOW = "fastersparql.op.distinct.window";

    /* --- --- --- default values --- --- --- */
    public static final int DEF_OP_DISTINCT_WINDOW = 16384;

    /* --- --- --- accessors --- --- --- */

    /**
     * If a window-based implementation of {@link Distinct} is used, this property determines
     * the window size.
     *
     * A window size of {@code n} means the last {@code n} elements are kept in a set (e.g., a
     * {@link java.util.HashSet}) and incomming elements are compared for uniqueness within that
     * set. If the set overgrows {@code n}, the oldest elements are removed.
     *
     * @return a positive ({@code n > 0}) integer.
     */
    public static @Positive int distinctWindow() {
        return readPositiveInt(OP_DISTINCT_WINDOW, DEF_OP_DISTINCT_WINDOW);
    }
}
