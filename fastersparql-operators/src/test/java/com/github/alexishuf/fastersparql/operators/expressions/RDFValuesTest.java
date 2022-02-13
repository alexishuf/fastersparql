package com.github.alexishuf.fastersparql.operators.expressions;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RDFValuesTest {
    static Stream<Arguments> testCoerceToBool() {
        return Stream.of(
                arguments(null, false),
                arguments(RDFValues.TRUE, true),
                arguments(RDFValues.FALSE, false),
                arguments("\"true\"^^<"+RDFValues.BOOLEAN+">", true),
                arguments("\"false\"^^<"+RDFValues.BOOLEAN+">", false),
                arguments("\"\"",      false),
                arguments("''",        false),
                arguments("\"1\"",     true),
                arguments("\"0\"",     false),
                arguments("\"false\"", false),
                arguments("\"False\"", false),
                arguments("\"true\"",  true),
                arguments("\"True\"",  true),
                arguments("\"23\"",    true),
                arguments("\"-1\"",    true),

                arguments("\"bullshit\"", null),
                arguments("\" \"",        null),

                arguments("\"0\"^^<"+RDFValues.integer+">", false),
                arguments("\"1\"^^<"+RDFValues.integer+">", true),
                arguments("\"23\"^^<"+RDFValues.integer+">", true),
                arguments("\"-1\"^^<"+RDFValues.integer+">", true),

                arguments("\"0\"^^<"+RDFValues.INT+">", false),
                arguments("\"1\"^^<"+RDFValues.INT+">", true),
                arguments("\"23\"^^<"+RDFValues.INT+">", true),
                arguments("\"-1\"^^<"+RDFValues.INT+">", true),

                arguments("\"0.0\"^^<"+RDFValues.decimal+">", false),
                arguments("\"1.0\"^^<"+RDFValues.decimal+">", true),
                arguments("\"23.0\"^^<"+RDFValues.decimal+">", true),
                arguments("\"-1.0\"^^<"+RDFValues.decimal+">", true),

                arguments("\"+23.0E-9\"^^<"+RDFValues.DOUBLE+">", true),
                arguments("\"+23.0E-9\"", true),
                arguments("\"+0.0E1\"", false)
        );
    }

    @ParameterizedTest @MethodSource
    void testCoerceToBool(String nt, Boolean expected) {
        if (expected == null)
            assertThrows(IllegalArgumentException.class, () -> RDFValues.coerceToBool(nt));
        else
            assertEquals(expected, RDFValues.coerceToBool(nt));
    }

}