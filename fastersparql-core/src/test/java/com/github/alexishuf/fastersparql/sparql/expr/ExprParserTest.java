package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.model.rope.RopeFactory;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.*;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExprParserTest {
    private record TestData(String expr, Vars vars, List<TestRow> rows) {
        public TestData(String expr, Term expected) {
            this(expr, Vars.EMPTY, List.of(new TestRow(List.of(), expected)));
        }

        public Term expected(int rowIdx) { return rows.get(rowIdx).expected; }
        public Binding binding(int rowIdx) {
            return new ArrayBinding(vars, rows.get(rowIdx).values);
        }
    }
    private record TestRow(List<Term> values, Term expected) { }

    static TestRow yes(String... terms) { return new TestRow(termList(terms), TRUE); }
    static TestRow  no(String... terms) { return new TestRow(termList(terms), FALSE); }

    @SuppressWarnings("unused") static Stream<Arguments> test() {
        var i1 = typed("1", DT_integer);
        var int23 = typed("23", DT_INT);
        var f23 = typed("23", DT_FLOAT);
        var db23 = typed("2.3e+1", DT_DOUBLE);
        var dc23 = typed("23.0", DT_decimal);
        List<TestData> rows = asList(
                // parse turtle-style numbers
                new TestData("1", typed("1", DT_integer)),
                new TestData("1.2", typed("1.2", DT_decimal)),
                new TestData("2.3e01", typed("2.3e01", DT_DOUBLE)),
                new TestData("2.3e+2", typed("2.3e+2", DT_DOUBLE)),
                new TestData("2.3E-2", typed("2.3E-2", DT_DOUBLE)),

                // accept ", ', """ and '''
                new TestData("\"23\"^^<http://www.w3.org/2001/XMLSchema#int>",
                             typed("23", DT_INT)),
                new TestData("'23'^^<http://www.w3.org/2001/XMLSchema#int>",
                             typed("23", DT_INT)),
                new TestData("\"\"\"23\"\"\"^^<http://www.w3.org/2001/XMLSchema#int>",
                             typed("23", DT_INT)),
                new TestData("'''23'''^^<http://www.w3.org/2001/XMLSchema#int>",
                             typed("23", DT_INT)),

                //accept ^^xsd: datatype
                new TestData("'23'^^xsd:int", typed("23", DT_INT)),

                //parse single-char plain strings
                new TestData("\"a\"", typed("a", DT_string)),
                new TestData("\"\"\"a\"\"\"", typed("a", DT_string)),
                new TestData("'a'", typed("a", DT_string)),
                new TestData("'''a'''", typed("a", DT_string)),

                //parse single-char typed strings
                new TestData("\"a\"^^<http://www.w3.org/2001/XMLSchema#string>", plainString("a")),
                new TestData("\"\"\"a\"\"\"^^<http://www.w3.org/2001/XMLSchema#string>", plainString("a")),
                new TestData("'a'^^<http://www.w3.org/2001/XMLSchema#string>", plainString("a")),
                new TestData("'''a'''^^<http://www.w3.org/2001/XMLSchema#string>", plainString("a")),

                //parse single-char xsd:string-typed strings
                new TestData("\"a\"^^xsd:string", plainString("a")),
                new TestData("\"\"\"a\"\"\"^^xsd:string", plainString("a")),
                new TestData("'a'^^xsd:string", plainString("a")),
                new TestData("'''a'''^^xsd:string", plainString("a")),

                //parse single-char lang-tagged strings
                new TestData("\"a\"@en", lang("a", "en")),
                new TestData("\"\"\"a\"\"\"@en-US", lang("a", "en-US")),
                new TestData("'a'@en-US", lang("a", "en-US")),
                new TestData("'''a'''@en", lang("a", "en")),

                //parse empty strings
                new TestData("\"\"", plainString("")),
                new TestData("\"\"\"\"\"\"", plainString("")),
                new TestData("''", plainString("")),
                new TestData("''''''", plainString("")),

                //parse empty typed strings
                new TestData("\"\"^^<http://www.w3.org/2001/XMLSchema#string>", plainString("")),
                new TestData("\"\"\"\"\"\"^^<http://www.w3.org/2001/XMLSchema#string>", plainString("")),
                new TestData("''^^<http://www.w3.org/2001/XMLSchema#string>", plainString("")),
                new TestData("''''''^^<http://www.w3.org/2001/XMLSchema#string>", plainString("")),

                //parse empty xsd-typed strings
                new TestData("\"\"^^xsd:string", plainString("")),
                new TestData("\"\"\"\"\"\"^^xsd:string", plainString("")),
                new TestData("''^^xsd:string", plainString("")),
                new TestData("''''''^^xsd:string", plainString("")),

                //parse empty lang-tagged strings
                new TestData("\"\"@en-US", lang("", "en-US")),
                new TestData("\"\"\"\"\"\"@en", lang("", "en")),
                new TestData("''@en", lang("", "en")),
                new TestData("''''''@en-US", lang("", "en-US")),

                //parse quote strings
                new TestData("\"\\\"\"@en-US", lang("\\\"", "en-US")),
                new TestData("\"\"\"\\\"\"\"\"@en", lang("\\\"", "en")),
                new TestData("'\\''@en", lang("\\'", "en")),
                new TestData("'''\\''''@en-US", lang("\\'", "en-US")),
                new TestData("'''''''@en-US", lang("'", "en-US")),

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
                new TestData("2 + 3", typed("5", DT_integer)),
                new TestData("2 - 3", typed("-1", DT_integer)),
                new TestData("2 * 3", typed("6", DT_integer)),
                new TestData("2 / 3", typed("0", DT_integer)),
                new TestData("6 / 2", typed("3", DT_integer)),
                new TestData("6 / 3", typed("2", DT_integer)),
                new TestData("3 / 2", typed("1", DT_integer)),
                //remove spaces
                new TestData("2+3", typed("5", DT_integer)),
                new TestData("2-3", typed("-1", DT_integer)),
                new TestData("2*3", typed("6", DT_integer)),
                new TestData("2/3", typed("0", DT_integer)),
                new TestData("6/2", typed("3", DT_integer)),
                new TestData("6/3", typed("2", DT_integer)),
                new TestData("6/3", typed("2", DT_integer)),
                new TestData("3/2", typed("1", DT_integer)),
                //parse math binary operators with decimal
                new TestData("2.0 + 3",   typed("5", DT_decimal)),
                new TestData("2   - 3.0", typed("-1", DT_decimal)),
                new TestData("2.0 * 3",   typed("6", DT_decimal)),
                new TestData("1   / 2.0", typed("0.5", DT_decimal)),
                new TestData("1.0 / 2",   typed("0.5", DT_decimal)),
                // remove spaces
                new TestData("2.0+3", typed("5", DT_decimal)),
                new TestData("2-3.0", typed("-1", DT_decimal)),
                new TestData("2.0*3", typed("6", DT_decimal)),
                new TestData("1/2.0", typed("0.5", DT_decimal)),
                new TestData("1.0/2", typed("0.5", DT_decimal)),

                //test precedence
                new TestData("1 + 2 * 3", typed("7", DT_integer)),
                new TestData("1 + 2 * 4 / 2", typed("5", DT_integer)),
                new TestData("1 + 2 * 4.0 / 2", typed("5", DT_decimal)),
                //remove spaces
                new TestData("1+2*3", typed("7", DT_integer)),
                new TestData("1+2*4/2", typed("5", DT_integer)),
                new TestData("1+2*4.0/2", typed("5", DT_decimal)),

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
                        List.of(new TestRow(termList("\"air end and\""),
                                            plainString("bir end bnd")),
                                new TestRow(termList("\"Air end and\"@en"),
                                            lang("Air end bnd", "en"))))
        );
        return rows.stream().map(Arguments::arguments);
    }

    private static final List<BatchType<?>> BATCH_TYPES = List.of(TermBatchType.TERM, CompressedBatchType.COMPRESSED);

    @ParameterizedTest @MethodSource
    void test(TestData data) {
        try (var parserGuard = new Guard<ExprParser>(this);
             var rope = PooledMutableRope.get()) {
            ExprParser parser = parserGuard.set(ExprParser.create());
            rope.append(data.expr);
            var paddedRope = RopeFactory.make(2+rope.len).add('!').add(rope).add('>').take();
            var subRope = paddedRope.sub(1, paddedRope.len-1);
            var bufferRope = new FinalSegmentRope(ByteBuffer.wrap(rope.toArray(0, rope.len)));

            var e1 = parser.parse(rope);
            var e2 = parser.parse(subRope);
            var e3 = parser.parse(bufferRope);
            assertEquals(e1, e2);
            assertEquals(e1, e3);
            assertEquals(e2, e3);

            for (Expr e : List.of(e1, e2, e3)) {
                for (int i = 0, size = data.rows.size(); i < size; ++i) {
                    Binding binding = data.binding(i);
                    assertEquals(data.expected(i), e.eval(binding), "at row " + i);
                    var evaluator = e.evaluator(binding.vars());
                    for (BatchType<?> bt : BATCH_TYPES) {
                        var b = bt.create(binding.vars().size()).takeOwnership(this);
                        try {
                            b.beginPut();
                            b.commitPut();
                            b.beginPut();
                            for (int col = 0; col < binding.vars().size(); col++)
                                b.putTerm(col, binding.get(col));
                            b.commitPut();
                            assertEquals(data.expected(i), evaluator.evaluate(b, 1),
                                    "at row " + i + ", bt=" + bt);
                        } finally {
                            b.recycle(this);
                        }
                    }
                }
            }
        }
    }

}