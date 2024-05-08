package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.asFinal;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ExprTest {
    static Stream<Arguments> testVars() {
        return Stream.of(
                arguments("23", Vars.EMPTY),
                arguments("?x", Vars.of("x")),
                arguments("$2", Vars.of("2")),
                arguments("23 > 2", Vars.EMPTY),
                arguments("REGEX(?t, 'asd')", Vars.of("t")),
                arguments("REGEX(?t, 'asd', ?flags) || ?x > ?y", Vars.of("t", "flags", "x", "y")),
                arguments("REGEX(?t, 'asd', ?flags) || ?x > ?y/?z - ?w", Vars.of("t", "flags", "x", "y", "z", "w")),
                arguments("REGEX(?t, 'asd', if(?hasFlags, ?flags, '')) || ?x > ?y/?z - ?w", Vars.of("t", "hasFlags", "flags", "x", "y", "z", "w"))
        );
    }

    @ParameterizedTest @MethodSource
    void testVars(String exprString, Vars expected) {
        Expr e;
        try (var parserGuard = new Guard<ExprParser>(this)) {
            e = parserGuard.set(ExprParser.create()).parse(asFinal(exprString));
        }
        Vars.Mutable vars = new Vars.Mutable(10);
        assertEquals(expected.size(), Expr.addVars(vars, e));
        assertEquals(expected, vars);
        assertEquals(0, Expr.addVars(vars, e));
    }

}