package com.github.alexishuf.fastersparql.sparql.results.serializer;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.*;
import static com.github.alexishuf.fastersparql.util.Results.results;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ResultsSerializerTest {
    private static Map<SparqlResultFormat, ResultsSerializer> serializers;

    @BeforeAll
    static void beforeAll() {
        serializers = new HashMap<>();
        serializers.put(JSON, ResultsSerializer.create(JSON));
        serializers.put(TSV, ResultsSerializer.create(TSV));
        serializers.put(WS, ResultsSerializer.create(WS));
    }


    static Stream<Arguments> test() {
        Results wide = results("?x ?y ?z", "exns:Bob", "\"bob\"@en", 23);
        Results tall = results("?x", "exns:Bob", "\"bob\"@en", 23, "_:b1");
        Results square = results("?x         ?y",
                                 "exns:Bob", "\"bob\"",
                                 "_:b2",     "-1.23");
        Results empty = results("?x");
        record D(Results results,  SparqlResultFormat fmt, String ex) {}
        List<D> data = new ArrayList<>(List.of(
                new D(Results.positiveResult(), TSV, "\n\n"),
                new D(Results.negativeResult(), TSV, "\n"),
                new D(Results.positiveResult(), WS, "\n\n!end\n"),
                new D(Results.negativeResult(), WS, "\n!end\n"),
                new D(Results.positiveResult(), JSON, """
                        {"head":{"vars":[]},
                         "boolean":true
                        }"""),
                new D(Results.negativeResult(), JSON, """
                        {"head":{"vars":[]},
                         "boolean":false
                        }"""),

                new D(empty, TSV, "?x\n"),
                new D(empty, WS, "?x\n!end\n"),
                new D(empty, JSON, """
                        {"head":{"vars":["x"]},
                        "results":{"bindings":[
                        ]}}"""),

                new D(wide, TSV, """
                        ?x\t?y\t?z
                        <http://www.example.org/ns#Bob>\t"bob"@en\t"23"^^<http://www.w3.org/2001/XMLSchema#integer>
                        """),
                new D(wide, WS, """
                        ?x\t?y\t?z
                        !prefix p2:<http://www.example.org/ns#>
                        p2:Bob\t"bob"@en\t23
                        !end
                        """),
                new D(wide, JSON, """
                        {"head":{"vars":["x","y","z"]},
                        "results":{"bindings":[
                        {"x":{"type":"iri", "value":"http://www.example.org/ns#Bob"},
                         "y":{"type":"literal", "value":"bob", "xml:lang":"en"},
                         "z":{"type":"literal", "value":"23", "datatype":"http://www.w3.org/2001/XMLSchema#integer"}}
                        ]}}"""),

                new D(tall, TSV, """
                        ?x
                        <http://www.example.org/ns#Bob>
                        "bob"@en
                        "23"^^<http://www.w3.org/2001/XMLSchema#integer>
                        _:b1
                        """),
                new D(tall, WS, """
                        ?x
                        !prefix p2:<http://www.example.org/ns#>
                        p2:Bob
                        "bob"@en
                        23
                        _:b1
                        !end
                        """),
                new D(tall, JSON, """
                        {"head":{"vars":["x"]},
                        "results":{"bindings":[
                        {"x":{"type":"iri", "value":"http://www.example.org/ns#Bob"}},
                        {"x":{"type":"literal", "value":"bob", "xml:lang":"en"}},
                        {"x":{"type":"literal", "value":"23", "datatype":"http://www.w3.org/2001/XMLSchema#integer"}},
                        {"x":{"type":"bnode", "value":"b1"}}
                        ]}}"""),

                new D(square, TSV, """
                        ?x\t?y
                        <http://www.example.org/ns#Bob>\t"bob"
                        _:b2\t"-1.23"^^<http://www.w3.org/2001/XMLSchema#decimal>
                        """),
                new D(square, WS, """
                        ?x\t?y
                        !prefix p2:<http://www.example.org/ns#>
                        p2:Bob\t"bob"
                        _:b2\t-1.23
                        !end
                        """),
                new D(square, JSON, """
                        {"head":{"vars":["x","y"]},
                        "results":{"bindings":[
                        {"x":{"type":"iri", "value":"http://www.example.org/ns#Bob"},
                         "y":{"type":"literal", "value":"bob"}},
                        {"x":{"type":"bnode", "value":"b2"},
                         "y":{"type":"literal", "value":"-1.23", "datatype":"http://www.w3.org/2001/XMLSchema#decimal"}}
                        ]}}""")
        ));
        List<Arguments> args = new ArrayList<>();
        for (BatchType<? extends Batch<? extends Batch<?>>> type : List.of(Batch.TERM, Batch.COMPRESSED)) {
            for (D(var r,  var f, var e) : data) args.add(arguments(type, r, f, e));
        }
        return args.stream();
    }

    @ParameterizedTest @MethodSource
    void test(BatchType<?> type, Results results, SparqlResultFormat fmt, String expected) {
        // serialize using a single batch
        Batch<?> b = type.create(results.size(), results.vars().size());
        for (List<Term> row : results.expected()) b = b.putRow(row);
        var serializer = serializers.get(fmt);

        ByteRope out = new ByteRope();
        serializer.init(results.vars(), results.vars(), results.isAsk(), out);
        serializer.serialize(b, out);
        serializer.serializeTrailer(out);

        assertEquals(expected, out.toString());

        //serialize again with a single batch but skipping fir column and first row
        b = b.clear(results.vars().size()+1).beginPut();
        for (int i = 0; i < b.cols; i++) b.putTerm(i, Term.RDF_SEQ);
        b.commitPut();
        for (List<Term> row : results.expected()) {
            (b = b.beginPut()).putTerm(0, Term.XSD_ANYURI);
            int c = 1;
            for (Term t : row)  b.putTerm(c++, t);
            b.commitPut();
        }
        serializer.init(Vars.of("dummy").union(results.vars()), results.vars(),
                        results.isAsk(), out.clear());
        serializer.serialize(b, 1, b.rows-1, out);
        serializer.serializeTrailer(out);
        assertEquals(expected, out.toString());

        // serialize a sequence of singleton batches
        serializer.init(results.vars(), results.vars(), results.isAsk(), out.clear());
        for (List<Term> row : results.expected())
            serializer.serialize(b = b.clear(row.size()).putRow(row), out);
        serializer.serializeTrailer(out);
        assertEquals(expected, out.toString());

        // serialize a sequence of two-row batches, always skipping first row and column
        serializer.init(Vars.of("dummy").union(results.vars()), results.vars(),
                        results.isAsk(), out.clear());
        for (List<Term> row : results.expected()) {
            b = b.clear(row.size()+1).beginPut();
            for (int i = 0; i < b.cols; i++)  b.putTerm(i, Term.XSD_SHORT);
            b.commitPut();
            int c = 0;
            (b = b.beginPut()).putTerm(c++, Term.XSD_BOOLEAN);
            for (Term t : row) b.putTerm(c++, t);
            b.commitPut();
            serializer.serialize(b, 1, 1, out);
        }
        serializer.serializeTrailer(out);
        assertEquals(expected, out.toString());
        b.recycle();
    }

}