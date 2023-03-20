package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

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
        for (BatchType<?> rt : List.of(Batch.TERM, Batch.TERM, Batch.COMPRESSED)) {
            for (D d : list)
                args.add(arguments(d.results, d.expected, rt));
        }
        return args.stream();
    }

    @ParameterizedTest @MethodSource
    <B extends Batch<B>> void testSerialize(Results in, String expected,
                                            BatchType<B> batchType) {
        var serializer = new WsSerializer<B>(in.vars());
        var b = Batch.TERM.createSingleton(in.vars().size());
        var rows = in.expected();
        var actual = new StringBuilder();

        //feed single batch with first row
        if (!rows.isEmpty()) {
            b.putRow(rows.get(0));
            actual.append(serializer.serialize(batchType.convert(b)));
        }
        // feed a batch with rows [1,rows.size()-1)
        if (rows.size() > 2) {
            b.clear();
            for (int i = 1; i < rows.size()-1; i++)
                b.putRow(rows.get(i));
            actual.append(serializer.serialize(batchType.convert(b)));
        }
        // feed a batch with the last row if it is not also the first
        if (rows.size() > 1) {
            b.clear();
            b.putRow(rows.get(rows.size()-1));
            actual.append(serializer.serialize(batchType.convert(b)));
        }
        // feeding a terminal batch has no effect
        actual.append(serializer.serialize(null));
        actual.append("!end\n");

        assertEquals(expected, actual.toString());
    }


}