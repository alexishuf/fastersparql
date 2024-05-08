package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.sparql.results.serializer.WsSerializer;
import com.github.alexishuf.fastersparql.util.Results;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer.recycler;
import static com.github.alexishuf.fastersparql.util.Results.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class WsSerializerTest {

    public static Stream<Arguments> testSerialize() {
        record D(Results results, String expected) {}
        List<D> list = List.of(
                new D(negativeResult(), "\n!end\n"),
                new D(positiveResult(), "\n\n!end\n"),
                new D(results(Vars.of("x")), "?x\n!end\n"),
                new D(results(Vars.of("x", "y")), "?x\t?y\n!end\n"),
                new D(results(Vars.of("x", "longVar")), "?x\t?longVar\n!end\n"),

                // TTL syntax
                new D(results("?x", "1", "-1", "23789"), "?x\n1\n-1\n23789\n!end\n"),
                new D(results("?x", "?y",
                                "23.0", "23.7",
                                null, "1.2e02",
                                "-2.3e-02", null),
                        """
                        ?x\t?y
                        23.0\t23.7
                        \t1.2e02
                        -2.3e-02\t
                        !end
                        """),
                new D(results("?x", "true", "false", "_:bn"),
                        "?x\ntrue\nfalse\n_:bn\n!end\n"),

                // RDF and XSD are implicit
                new D(results("?x", "xsd:int", "rdf:type", "\"23\"^^xsd:int"),
                        "?x\nxsd:int\na\n\"23\"^^xsd:int\n!end\n"),

                // serialize shortened IRI
                new D(results("?x", "?y", "exns:Alice", "rdf:type"),
                        """
                        ?x\t?y
                        !prefix p2:<http://www.example.org/ns#>
                        p2:Alice\ta
                        !end
                        """),
                new D(results("?x", "?y", "xsd:int", "exns:Bob"),
                      """
                      ?x\t?y
                      !prefix p2:<http://www.example.org/ns#>
                      xsd:int\tp2:Bob
                      !end
                      """),
                new D(results("?x", "?y", null, "exns:Alice", "xsd:int", "owl:Class"),
                        """
                        ?x\t?y
                        !prefix p2:<http://www.example.org/ns#>
                        \tp2:Alice
                        !prefix p3:<http://www.w3.org/2002/07/owl#>
                        xsd:int\tp3:Class
                        !end
                        """),

                // serialize shortened literal
                new D(results("?x", "?y", "\"23\"^^xsd:int", null, null, "\"<p>\"^^rdf:HTML"),
                        """
                        ?x\t?y
                        "23"^^xsd:int\t
                        \t"<p>"^^rdf:HTML
                        !end
                        """)
        );

        List<Arguments> args = new ArrayList<>();
        for (BatchType<?> bt : List.of(TermBatchType.TERM, TermBatchType.TERM, CompressedBatchType.COMPRESSED)) {
            for (var d : list)
                args.add(arguments(d.results, d.expected, bt));
        }
        return args.stream();
    }

    @ParameterizedTest @MethodSource
    <B extends Batch<B>> void testSerialize(Results in, String expected,
                                            BatchType<B> batchType) {
        var rows       = in.expected();
        int hardMax    = expected.length();
        var recycler   = recycler(batchType);
        try (var bGuard = new Guard.BatchGuard<TermBatch>(this);
             var actual = PooledMutableRope.get();
             var sink = PooledMutableRope.get();
             var serializerGuard = new Guard<WsSerializer>(this)) {
            var serializer = serializerGuard.set(WsSerializer.create(256));
            ResultsSerializer.ChunkConsumer<FinalSegmentRope> appender = actual::append;
            serializer.init(in.vars(), in.vars(), false);
            serializer.serializeHeader(actual);
            var b = bGuard.set(TermBatchType.TERM.create(in.vars().size()));

            //feed single batch with first row
            if (!rows.isEmpty()) {
                b.putRow(rows.getFirst());
                serializer.serialize(batchType.convertOrCopy(b), sink, hardMax, recycler, appender);
            }
            // feed a batch with rows [1,rows.size()-1)
            if (rows.size() > 2) {
                b.clear();
                for (int i = 1; i < rows.size()-1; i++)
                    b.putRow(rows.get(i));
                serializer.serialize(batchType.convertOrCopy(b), sink, hardMax, recycler, appender);
            }
            // feed a batch with the last row if it is not also the first
            if (rows.size() > 1) {
                b.clear();
                b.putRow(rows.getLast());
                serializer.serialize(batchType.convertOrCopy(b), sink, hardMax, recycler, appender);
            }
            // feeding an empty batch has no effect
            serializer.serialize(batchType.create(in.vars().size()),
                    sink, hardMax, recycler, appender);
            // feeding a null batch has no effect
            serializer.serialize(null, sink, hardMax, recycler, appender);
            serializer.serializeTrailer(actual);

            assertEquals(expected, actual.toString());
        }
    }


}