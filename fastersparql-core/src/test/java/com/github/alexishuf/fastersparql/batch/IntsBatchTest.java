package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.*;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class IntsBatchTest {
    @ParameterizedTest @ValueSource(ints = {0, 1, 256, 512, 1024, 1_000})
    void testTerm(int i) {
        assertEquals(Term.typed(("\""+i).getBytes(UTF_8), DT_integer), term(i));
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 4, 256, 1_000})
    void testInts(int size) {
        int[] a0 = IntsBatch.ints(size);
        int[] a1 = IntsBatch.ints(size);
        int[] a2 = IntsBatch.ints(0, size);
        int[] b  = IntsBatch.ints(1, size);
        assertSame(a0, a1);
        assertSame(a0, a2);
        for (int i = 0; i < a0.length; i++)
            assertEquals(i, a0[i]);
        for (int i = 0; i < b.length; i++)
            assertEquals(i+1, b[i]);
    }

    static Stream<Arguments> testParse() {
        return Stream.of(
                arguments(0, Term.typed("\"0".getBytes(UTF_8), DT_integer)),
                arguments(9, Term.typed("\"9".getBytes(UTF_8), DT_integer)),
                arguments(10, Term.typed("\"10".getBytes(UTF_8), DT_integer)),
                arguments(19, Term.typed("\"19".getBytes(UTF_8), DT_integer)),
                arguments(157, Term.typed("\"157".getBytes(UTF_8), DT_integer)),

                arguments(-1, Term.typed("\"-1".getBytes(UTF_8), DT_integer)),
                arguments(-9, Term.typed("\"-9".getBytes(UTF_8), DT_integer)),
                arguments(-10, Term.typed("\"-10".getBytes(UTF_8), DT_integer)),
                arguments(-19, Term.typed("\"-19".getBytes(UTF_8), DT_integer)),
                arguments(-157, Term.typed("\"-157".getBytes(UTF_8), DT_integer))
        );
    }

    @ParameterizedTest @MethodSource
    void testParse(int ex, Term term) {
        assertEquals(ex, IntsBatch.parse(term));
    }

    @Test void testFill() {
        TermBatch ex = TermBatch.of(termList(1), termList(2),  termList(3));
        var b = IntsBatch.fill(new TermBatch(new Term[3], 0, 1, true), 1, 2, 3);
        assertEquals(ex, b);
    }

    static Stream<Arguments> testHistogram() {
        return Stream.of(
                arguments(ints(0), 0, new int[]{}),
                arguments(ints(1), 1, new int[]{1}),
                arguments(ints(2), 1, new int[]{1}),
                arguments(ints(3), 2, new int[]{1, 1}),
                arguments(ints(7), 7, new int[]{1, 1, 1, 1, 1, 1, 1}),

                arguments(new int[] {1, 2, 1}, 3, new int[] {0, 2, 1}),
                arguments(new int[] {0, 2, 0}, 3, new int[] {2, 0, 1})
        );
    }

    @ParameterizedTest @MethodSource
    void testHistogram(int[] ints, int size, int[] expected) {
        assertArrayEquals(expected, histogram(ints, size));
    }
}
