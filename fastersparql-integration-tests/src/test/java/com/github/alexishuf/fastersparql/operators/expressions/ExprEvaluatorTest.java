package com.github.alexishuf.fastersparql.operators.expressions;

import com.github.alexishuf.fastersparql.operators.row.impl.ListOperations;
import lombok.Value;
import lombok.experimental.Accessors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.expressions.RDFValues.decimal;
import static com.github.alexishuf.fastersparql.operators.expressions.RDFValues.integer;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ExprEvaluatorTest {
    private static final List<ExprEvaluatorCompilerProvider> allProviders =
            ExprEvaluatorCompilerRegistry.get().allProviders();

    @Value @Accessors(fluent = true)
    private static class TestData {
        String expr;
        List<String> vars;
        List<TestRow> rows;
    }

    @Value @Accessors(fluent = true)
    private static class TestRow {
        List<String> values;
        String expected;
    }

    private static String integer(int i) {
        return String.format("\"%d\"^^<%s>", i, integer);
    }

    public static TestRow yes(String... terms) {
        return new TestRow(asList(terms), RDFValues.TRUE);
    }
    public static TestRow no(String... terms) {
        return new TestRow(asList(terms), RDFValues.FALSE);
    }

    @SuppressWarnings("unused") static Stream<Arguments> test() {
        List<TestData> rows = asList(
        /*  1 */new TestData("?x < ?y", asList("x", "y"),
                             asList(yes(integer(1), integer(2)),
                                     no(integer(2), integer(1)))),
        /*  2 */new TestData("?y < ?x", asList("y", "x"),
                             asList(yes(integer(1), integer(2)),
                                     no(integer(2), integer(1)))),
        /*  3 */new TestData("?x < ?y", asList("y", "x"),
                             asList( no(integer(1), integer(2)),
                                    yes(integer(2), integer(1)))),
        /*  4 */new TestData("?x = ?y", asList("x", "y"),
                             asList(yes(integer(1), integer(1)),      // 0
                                     no(integer(1), integer(2)),      // 1
                                    yes("\"bob\"", "\"bob\""),        // 2
                                    no("\"bob\"", "\"bob\"@en"),      // 3
                                    yes("\"bob\"@en", "\"bob\"@en"))),// 4
        /*  5 */new TestData("?x > 23", singletonList("x"),
                             asList( no(integer(7)),
                                    yes(integer(27)),
                                    yes("\"27.4\"^^<"+decimal+">"),
                                     no("\"22.6\"^^<"+decimal+">")))
        );
        List<ExprEvaluatorCompiler> compilers =
                allProviders.stream().map(ExprEvaluatorCompilerProvider::get).collect(toList());
        assertFalse(compilers.isEmpty());
        return compilers.stream().flatMap(compiler ->
                rows.stream().map(data -> arguments(compiler, data)));
    }

    @ParameterizedTest @MethodSource
    void test(ExprEvaluatorCompiler compiler, TestData data) {
        ExprEvaluator<List<String>> evaluator =
                compiler.compile(List.class, ListOperations.get(), data.vars, data.expr);
        for (int i = 0, size = data.rows.size(); i < size; ++i) {
            TestRow row = data.rows.get(i);
            assertEquals(row.expected, evaluator.evaluate(row.values), "at row "+i);
        }
    }

}