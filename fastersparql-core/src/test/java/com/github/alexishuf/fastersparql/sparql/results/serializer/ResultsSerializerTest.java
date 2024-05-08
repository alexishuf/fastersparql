package com.github.alexishuf.fastersparql.sparql.results.serializer;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.Results;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.*;
import static com.github.alexishuf.fastersparql.util.Results.results;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ResultsSerializerTest {
    private static Map<SparqlResultFormat, ResultsSerializer<?>> serializers;

    @BeforeAll static void beforeAll() {
        serializers = new HashMap<>();
        var cls = ResultsSerializerTest.class;
        serializers.put(JSON, ResultsSerializer.create(JSON).takeOwnership(cls));
        serializers.put(TSV,  ResultsSerializer.create(TSV ).takeOwnership(cls));
        serializers.put(WS,   ResultsSerializer.create(WS  ).takeOwnership(cls));
    }

    @AfterAll static void afterAll() {
        for (var it = serializers.entrySet().iterator(); it.hasNext(); ) {
            it.next().getValue().recycle(ResultsSerializerTest.class);
            it.remove();
        }
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
        for (BatchType<? extends Batch<? extends Batch<?>>> type : List.of(TermBatchType.TERM, CompressedBatchType.COMPRESSED)) {
            for (var d : data) args.add(arguments(type, d.results, d.fmt, d.ex));
        }
        return args.stream();
    }

    @ParameterizedTest @MethodSource
    <B extends Batch<B>, S extends ResultsSerializer<S>>
    void test(BatchType<B> type, Results results, SparqlResultFormat fmt, String expected) {
        // serialize using a single batch
        try (var bGuard = new Guard.BatchGuard<B>(this);
             var out = PooledMutableRope.get()) {
            B b = bGuard.set(type.create(results.vars().size()));
            for (List<Term> row : results.expected()) b.putRow(row);
            @SuppressWarnings("unchecked") var serializer = (S)serializers.get(fmt);

            serializer.init(results.vars(), results.vars(), results.isAsk());
            serializer.serializeHeader(out);
            try (var reassemble = new ResultsSerializer.Reassemble<B>();
                 var sink = PooledMutableRope.get()) {
                serializer.serialize(bGuard.take(), sink,
                        expected.length(), reassemble, out::append);
                b = bGuard.set(Objects.requireNonNull(reassemble.take()));
            }
            serializer.serializeTrailer(out);
            assertEquals(expected, out.toString());

            // previous reassemble should yield unchanged b
            for (int i = 0; i < 2; ++i) {
                serializer.init(results.vars(), results.vars(), results.isAsk());
                serializer.serializeHeader(out.clear());
                serializer.serialize(b, this, out); // must not change b
                serializer.serializeTrailer(out);
                assertEquals(expected, out.toString(), "i="+i);
                assertTrue(b.isOwner(this)); // ownership unchanged/restored
            }

            // serialize a sequence of singleton batches
            serializer.init(results.vars(), results.vars(), results.isAsk());
            serializer.serializeHeader(out.clear());
            for (List<Term> row : results.expected()) {
                b.clear(row.size()).putRow(row);
                try (var reassemble = new ResultsSerializer.Reassemble<B>();
                     var sink = PooledMutableRope.get()) {
                    serializer.serialize(bGuard.take(), sink,
                            expected.length()-1, reassemble, out::append);
                    b = bGuard.set(Objects.requireNonNull(reassemble.take()));
                }
            }
            serializer.serializeTrailer(out);
            assertEquals(expected, out.toString());

            // serialize a sequence of at most 2 row batches, always skipping first column
            serializer.init(Vars.of("dummy").union(results.vars()), results.vars(),
                    results.isAsk());
            serializer.serializeHeader(out.clear());
            for (int r = 0, rows = results.expected().size(); r < rows; r += 2) {
                b.clear(results.vars().size()+1);
                for (int rOff = 0, rCount = r+1 < rows ? 2 : 1; rOff < rCount; rOff++) {
                    List<Term> row = results.expected().get(r+rOff);
                    int c = 0;
                    b.beginPut();
                    b.putTerm(c++, Term.XSD_BOOLEAN);
                    for (Term t : row) b.putTerm(c++, t);
                    b.commitPut();
                }
                try (var reassemble = new ResultsSerializer.Reassemble<B>();
                     var sink = PooledMutableRope.get()) {
                    serializer.serialize(bGuard.take(), sink,
                            expected.length(), reassemble, out::append);
                    b = bGuard.set(Objects.requireNonNull(reassemble.take()));
                }
            }
            serializer.serializeTrailer(out);
            assertEquals(expected, out.toString());
        }
    }

}