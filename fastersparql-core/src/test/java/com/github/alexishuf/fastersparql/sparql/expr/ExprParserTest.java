package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.sparql.RDFTypes;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.binding.ListBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term.Lit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.sparql.RDFTypes.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Lit.FALSE;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Lit.TRUE;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExprParserTest {
    private record TestData(String expr, Vars vars, List<TestRow> rows) {
        public TestData(String expr, Lit expected) {
            this(expr, Vars.EMPTY, List.of(new TestRow(List.of(), expected)));
        }

        public Term expected(int rowIdx) { return rows.get(rowIdx).expected; }
        public Binding binding(int rowIdx) {
            return ListBinding.wrap(vars, rows.get(rowIdx).values);
        }
    }
    private record TestRow(List<String> values, Term expected) { }

    public static TestRow yes(String... terms) { return new TestRow(asList(terms), TRUE); }
    public static TestRow  no(String... terms) { return new TestRow(asList(terms), FALSE); }

    @SuppressWarnings("unused") static Stream<Arguments> test() {
        var i1 = new Lit("1", RDFTypes.integer, null);
        var int23 = new Lit("23", INT, null);
        var f23 = new Lit("23", RDFTypes.FLOAT, null);
        var db23 = new Lit("2.3e+1", RDFTypes.DOUBLE, null);
        var dc23 = new Lit("23.0", decimal, null);
        List<TestData> rows = asList(
                // parse turtle-style numbers
                new TestData("1", new Lit("1", integer, null)),
                new TestData("1.2", new Lit("1.2", decimal, null)),
                new TestData("2.3e01", new Lit("2.3e01", DOUBLE, null)),
                new TestData("2.3e+2", new Lit("2.3e+2", DOUBLE, null)),
                new TestData("2.3E-2", new Lit("2.3E-2", DOUBLE, null)),

                // accept ", ', """ and '''
                new TestData("\"23\"^^<http://www.w3.org/2001/XMLSchema#int>",
                             new Lit("23", INT, null)),
                new TestData("'23'^^<http://www.w3.org/2001/XMLSchema#int>",
                             new Lit("23", INT, null)),
                new TestData("\"\"\"23\"\"\"^^<http://www.w3.org/2001/XMLSchema#int>",
                        new Lit("23", INT, null)),
                new TestData("'''23'''^^<http://www.w3.org/2001/XMLSchema#int>",
                        new Lit("23", INT, null)),

                //accept ^^xsd: datatype
                new TestData("'23'^^xsd:int", new Lit("23", INT, null)),

                //parse single-char plain strings
                new TestData("\"a\"", new Lit("a", string, null)),
                new TestData("\"\"\"a\"\"\"", new Lit("a", string, null)),
                new TestData("'a'", new Lit("a", string, null)),
                new TestData("'''a'''", new Lit("a", string, null)),

                //parse single-char typed strings
                new TestData("\"a\"^^<http://www.w3.org/2001/XMLSchema#string>", new Lit("a", string, null)),
                new TestData("\"\"\"a\"\"\"^^<http://www.w3.org/2001/XMLSchema#string>", new Lit("a", string, null)),
                new TestData("'a'^^<http://www.w3.org/2001/XMLSchema#string>", new Lit("a", string, null)),
                new TestData("'''a'''^^<http://www.w3.org/2001/XMLSchema#string>", new Lit("a", string, null)),

                //parse single-char xsd:string-typed strings
                new TestData("\"a\"^^xsd:string", new Lit("a", string, null)),
                new TestData("\"\"\"a\"\"\"^^xsd:string", new Lit("a", string, null)),
                new TestData("'a'^^xsd:string", new Lit("a", string, null)),
                new TestData("'''a'''^^xsd:string", new Lit("a", string, null)),

                //parse single-char lang-tagged strings
                new TestData("\"a\"@en", new Lit("a", langString, "en")),
                new TestData("\"\"\"a\"\"\"@en-US", new Lit("a", langString, "en-US")),
                new TestData("'a'@en-US", new Lit("a", langString, "en-US")),
                new TestData("'''a'''@en", new Lit("a", langString, "en")),

                //parse empty strings
                new TestData("\"\"", new Lit("", string, null)),
                new TestData("\"\"\"\"\"\"", new Lit("", string, null)),
                new TestData("''", new Lit("", string, null)),
                new TestData("''''''", new Lit("", string, null)),

                //parse empty typed strings
                new TestData("\"\"^^<http://www.w3.org/2001/XMLSchema#string>", new Lit("", string, null)),
                new TestData("\"\"\"\"\"\"^^<http://www.w3.org/2001/XMLSchema#string>", new Lit("", string, null)),
                new TestData("''^^<http://www.w3.org/2001/XMLSchema#string>", new Lit("", string, null)),
                new TestData("''''''^^<http://www.w3.org/2001/XMLSchema#string>", new Lit("", string, null)),

                //parse empty xsd-typed strings
                new TestData("\"\"^^xsd:string", new Lit("", string, null)),
                new TestData("\"\"\"\"\"\"^^xsd:string", new Lit("", string, null)),
                new TestData("''^^xsd:string", new Lit("", string, null)),
                new TestData("''''''^^xsd:string", new Lit("", string, null)),

                //parse empty lang-tagged strings
                new TestData("\"\"@en-US", new Lit("", langString, "en-US")),
                new TestData("\"\"\"\"\"\"@en", new Lit("", langString, "en")),
                new TestData("''@en", new Lit("", langString, "en")),
                new TestData("''''''@en-US", new Lit("", langString, "en-US")),

                //parse quote strings
                new TestData("\"\\\"\"@en-US", new Lit("\\\"", langString, "en-US")),
                new TestData("\"\"\"\\\"\"\"\"@en", new Lit("\\\"", langString, "en")),
                new TestData("'\\''@en", new Lit("\\'", langString, "en")),
                new TestData("'''\\''''@en-US", new Lit("\\'", langString, "en-US")),
                new TestData("'''''''@en-US", new Lit("'", langString, "en-US")),

                //parse <, <=, =, >=, >
                new TestData("1 <  2", TRUE),
                new TestData("1 <= 2", TRUE),
                new TestData("2  = 2", TRUE),
                new TestData("2 >= 1", TRUE),
                new TestData("2 >  1", TRUE),
                new TestData("2 <  1", FALSE),
                new TestData("2 <= 1", FALSE),
                new TestData("1  = 2", FALSE),
                new TestData("1 >= 2", FALSE),
                new TestData("1 >  2", FALSE),
                //parse <, <=, =, >=, > with vars
                new TestData("?x < ?y", Vars.of("x", "y"),
                        List.of(yes("1", "2"),
                                 no("2", "1"),
                                 no("1", "1"))),
                new TestData("?x <= ?y", Vars.of("x", "y"),
                        List.of(yes("1", "2"),
                                 no("2", "1"),
                                yes("1", "1"))),
                new TestData("?x = ?y", Vars.of("x", "y"),
                        List.of( no("1", "2"),
                                 no("2", "1"),
                                yes("1", "1"))),
                new TestData("?x != ?y", Vars.of("x", "y"),
                        List.of(yes("1", "2"),
                                yes("2", "1"),
                                 no("1", "1"))),
                new TestData("?x >= ?y", Vars.of("x", "y"),
                        List.of( no("1", "2"),
                                yes("2", "1"),
                                yes("1", "1"))),
                new TestData("?x > ?y", Vars.of("x", "y"),
                        List.of( no("1", "2"),
                                yes("2", "1"),
                                 no("1", "1"))),

                //parse && and ||
                new TestData("true  && true",  TRUE),
                new TestData("true  && false", FALSE),
                new TestData("false && true",  FALSE),
                new TestData("false && false", FALSE),
                new TestData("true  || \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>",          TRUE),
                new TestData("true  || 'false'^^<http://www.w3.org/2001/XMLSchema#boolean>",           TRUE),
                new TestData("false || \"\"\"true\"\"\"^^<http://www.w3.org/2001/XMLSchema#boolean>",  TRUE),
                new TestData("false || '''false'''^^<http://www.w3.org/2001/XMLSchema#boolean>",       FALSE),
                new TestData("true  || \"true\"^^xsd:boolean",          TRUE),
                new TestData("true  || 'false'^^xsd:boolean",           TRUE),
                new TestData("false || \"\"\"true\"\"\"^^xsd:boolean",  TRUE),
                new TestData("false || '''false'''^^xsd:boolean",       FALSE),

                //parse math binary operators with integer
                new TestData("2 + 3", new Lit("5", integer, null)),
                new TestData("2 - 3", new Lit("-1", integer, null)),
                new TestData("2 * 3", new Lit("6", integer, null)),
                new TestData("2 / 3", new Lit("0", integer, null)),
                new TestData("6 / 2", new Lit("3", integer, null)),
                new TestData("6 / 3", new Lit("2", integer, null)),
                new TestData("3 / 2", new Lit("1", integer, null)),
                //remove spaces
                new TestData("2+3", new Lit("5", integer, null)),
                new TestData("2-3", new Lit("-1", integer, null)),
                new TestData("2*3", new Lit("6", integer, null)),
                new TestData("2/3", new Lit("0", integer, null)),
                new TestData("6/2", new Lit("3", integer, null)),
                new TestData("6/3", new Lit("2", integer, null)),
                new TestData("6/3", new Lit("2", integer, null)),
                new TestData("3/2", new Lit("1", integer, null)),
                //parse math binary operators with decimal
                new TestData("2.0 + 3",   new Lit("5", decimal, null)),
                new TestData("2   - 3.0", new Lit("-1", decimal, null)),
                new TestData("2.0 * 3",   new Lit("6", decimal, null)),
                new TestData("1   / 2.0", new Lit("0.5", decimal, null)),
                new TestData("1.0 / 2",   new Lit("0.5", decimal, null)),
                // remove spaces
                new TestData("2.0+3", new Lit("5", decimal, null)),
                new TestData("2-3.0", new Lit("-1", decimal, null)),
                new TestData("2.0*3", new Lit("6", decimal, null)),
                new TestData("1/2.0", new Lit("0.5", decimal, null)),
                new TestData("1.0/2", new Lit("0.5", decimal, null)),

                //test precedence
                new TestData("1 + 2 * 3", new Lit("7", integer, null)),
                new TestData("1 + 2 * 4 / 2", new Lit("5", integer, null)),
                new TestData("1 + 2 * 4.0 / 2", new Lit("5", decimal, null)),
                //remove spaces
                new TestData("1+2*3", new Lit("7", integer, null)),
                new TestData("1+2*4/2", new Lit("5", integer, null)),
                new TestData("1+2*4.0/2", new Lit("5", decimal, null)),

                // test dynamic regex
                new TestData("REGEX(\"abcde\", ?p, ?f)", Vars.of("p", "f"),
                        List.of(yes("'bc'", "''"),
                                 no("'cb'", "''"),
                                yes("'^a.c'", "''"),
                                yes("'^A.+C'", "'i'"),
                                 no("'^A.+C$'", "'i'"))),
                // precompiled regex
                new TestData("REGEX(?x, \"^A.c\", 'i')", Vars.of("x"),
                        List.of(yes("'abc'"),
                                yes("'abcd'"),
                                no("'-abcd'"),
                                yes("'aBCd'"))),

                // precompiled replace
                new TestData("REPLACE(?x, 'a(ir|nd)', 'b$1')", Vars.of("x"),
                        List.of(new TestRow(List.of("'air end and'"),
                                            new Lit("bir end bnd", string, null)),
                                new TestRow(List.of("\"Air end and\"@en"),
                                            new Lit("Air end bnd", langString, "en"))))
        );
        return rows.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void test(TestData data) {
        ExprParser parser = new ExprParser();
        var e1 = parser.parse(data.expr);
        var e2 = parser.parse(data.expr);
        //noinspection StringBufferReplaceableByString
        var e3 = parser.parse(new StringBuilder().append(data.expr).toString());
        assertEquals(e1, e2);
        assertEquals(e1, e3);
        for (Expr e : List.of(e1, e2, e3)) {
            for (int i = 0, size = data.rows.size(); i < size; ++i)
                assertEquals(data.expected(i), e.eval(data.binding(i)), "at row " + i);
        }
    }

}