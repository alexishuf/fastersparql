package com.github.alexishuf.fastersparql.operators.reorder;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class JoinReorderStrategyTest {
    @SuppressWarnings("unused") static Stream<Arguments> testLoadStrategy() {
        Class<?> n = NoneJoinReorderStrategy.class;
        Class<?> a = AvoidCartesianJoinReorderStrategy.class;
        return Stream.of(
                /*  1 */arguments("none", n),
                /*  2 */arguments("none ", n),
                /*  3 */arguments(" none ", n),
                /*  4 */arguments("None", n),
                /*  5 */arguments("None ", n),
                /*  6 */arguments("\tNone\n", n),
                /*  7 */arguments("AvoidCartesian", a),
                /*  8 */arguments("AvoidCartesian\n", a),
                /*  9 */arguments("bullshit", null)
        );
    }

    @ParameterizedTest @MethodSource
    public void testLoadStrategy(String name, Class<? extends JoinReorderStrategy> cls) {
        JoinReorderStrategy strategy = JoinReorderStrategy.loadStrategy(name);
        if (cls == null) {
            assertNull(strategy);
        } else {
            assertNotNull(strategy);
            assertEquals(cls, strategy.getClass());
        }
    }
}