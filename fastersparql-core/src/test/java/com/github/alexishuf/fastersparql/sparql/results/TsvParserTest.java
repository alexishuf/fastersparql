package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.Results.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TsvParserTest extends ResultsParserTest {

    public static Stream<Arguments> test() {
        String dec = "\"^^<http://www.w3.org/2001/XMLSchema#decimal>";
        String dbl = "\"^^<http://www.w3.org/2001/XMLSchema#double>";
        String bool = "\"^^<http://www.w3.org/2001/XMLSchema#boolean>";
        String i23  = "\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>";
        return Stream.of(
                /*  1 */arguments("", negativeResult()),
                /*  2 */arguments("\n", negativeResult()),
                /*  3 */arguments("\n\n", positiveResult()),
                /*  4 */arguments("?x\n", results(Vars.of("x"))),
                /*  5 */arguments("?x\n\n", results("?x", null)),
                /*  6 */arguments("?x\n<a>\n", results("?x", "<a>")),
                /*  7 */arguments("?x\n<a>", results("?x", "<a>")),
                /*  8 */arguments("?x\n<http://example.org/?page=A&x=y#[]>\n",
                                  results("?x", "<http://example.org/?page=A&x=y#[]>")),
                /*  9 */arguments("?x\n"+i23+"\n", results("?x", 23)),
                /* 10 */arguments("?x\n\"bob\"@en-US\n", results("?x", "\"bob\"@en-US")),
                /* 11 */arguments("?x\n\"bob\"\n", results("?x", "\"bob\"")),
                /* 12 */arguments("?x\t?y\n", results("?x", "?y")),
                /* 13 */arguments("?x\t?y", results("?x", "?y")),
                /* 14 */arguments("?x\t?y\n\t\n", results("?x", "?y", null, null)),
                /* 15 */arguments("?x\t?y\n\t\n\t\n", results("?x", "?y", null, null, null, null)),
                /* 16 */arguments("?x\t?y\n<a>\t<b>\n", results("?x", "?y", "<a>", "<b>")),
                /* 17 */arguments("?x\t?y\n<a>\t<b>", results("?x", "?y", "<a>", "<b>")),
                /* 18 */arguments("?x\t?y\n\"\\ta\"\t\"\\nb\"\n",
                                  results("?x", "?y", "\"\\ta\"", "\"\\nb\"")),
                /* 19 */arguments("?x\t?y\n\"\\ta\"\t\"\\nb\"\n<a>\t<b>\n",
                                  results("?x", "?y", "\"\\ta\"", "\"\\nb\"", "<a>",      "<b>")),
                /* 20 */arguments("x\n<a>", results("?x", "<a>")),
                /* 21 */arguments("x\ty\n\t<b>\n", results("?x", "?y", null, "<b>")),
                /* 22 */arguments("?x\n<a>\t<b>\n",
                                  results(Vars.of("x")).error(FSServerException.class)),
                /* 23 */arguments("?x\n\t<b>\n", results(Vars.of("x")).error(FSServerException.class)),
                /* 24 */arguments("?x\n<a>\n\t\t\n", results("?x", "<a>").error(FSServerException.class)),
                /* 25 */arguments("?x\n23", results("?x", 23)),
                /* 26 */arguments("?x\n+23", results("?x", 23)),
                /* 27 */arguments("?x\n-23", results("?x", -23)),
                /* 28 */arguments("?x\n1.23", results("?x", "\"1.23"+dec)),
                /* 29 */arguments("?x\n+1.23", results("?x", "\"1.23"+dec)),
                /* 30 */arguments("?x\n-1.33", results("?x", "\"-1.33"+dec)),
                /* 31 */arguments("?x\n1.33e6", results("?x", "\"1.33e6"+dbl)),
                /* 32 */arguments("?x\n1.33E6", results("?x", "\"1.33E6"+dbl)),
                /* 33 */arguments("?x\n+1.33e6", results("?x", "\"+1.33e6"+dbl)),
                /* 34 */arguments("?x\n-1.33e6", results("?x", "\"-1.33e6"+dbl)),
                /* 35 */arguments("?x\n-1.33e-06", results("?x", "\"-1.33e-06"+dbl)),
                /* 36 */arguments("?_askResult\n\"true"+bool, positiveResult()),
                /* 37 */arguments("?_askResult\ntrue\n", positiveResult()),
                /* 38 */arguments("?_askResult\n\"false"+bool, negativeResult()),
                /* 39 */arguments("?_askResult\nfalse\n", negativeResult()),
                /* 40 */arguments("?_askResult\n", negativeResult()),
                /* 41 */arguments("?_askResult\ntrue\ntrue", results("?_askResult", "true", "true")),
                /* 42 */arguments("?_askResult\n\nfalse", results("?_askResult", null, "false")),
                /* 43 */arguments("?__ask__\ntrue", positiveResult()),
                /* 44 */arguments("?__ask__\n\ntrue", results("?__ask__", null, "true"))
        );
    }

    @ParameterizedTest @MethodSource
    void test(CharSequence input, Results expected) throws Exception {
        doTest(new SVParserBIt.TsvFactory(), expected, Rope.of(input));
    }
}
