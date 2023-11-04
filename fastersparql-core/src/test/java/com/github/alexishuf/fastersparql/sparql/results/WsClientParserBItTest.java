package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.results.serializer.WsSerializer;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.Results.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class WsClientParserBItTest extends ResultsParserTest {
    public static Stream<Arguments> test() {
        return Stream.of(
                // negative ask results, tolerate missing header line and missing !end
                arguments(negativeResult(), "\n!end\n"),
                arguments(negativeResult(), "\n!end"),
                arguments(negativeResult(), "!end\n"),
                arguments(negativeResult(), "\n"),
                arguments(negativeResult(), ""),

                // positive ask result, tolerate missing !end
                arguments(positiveResult(), "\n\n!end\n"),
                arguments(positiveResult(), "\n\n!end"),
                arguments(positiveResult(), "\n\n"),

                // single-column results with NT syntax, tolerate missing !end
                arguments(results("?x", "_:x-1"), "?x\n_:x-1\n!end\n"),
                arguments(results("?x", "_:x-1"), "?x\n_:x-1\n!end"),
                arguments(results("?x", "_:x-1"), "?x\n_:x-1\n"),
                arguments(results("?x", "_:x-1"), "?x\n_:x-1"),
                arguments(results("?y", "_:y-1"), "?y\n_:y-1\n!end\n"),
                arguments(results("?1long_name", "_:1"), "?1long_name\n_:1\n!end\n"),

                // single-column, 2 rows, NT syntax
                arguments(results("?x", "_:x1", "_:x2"), "?x\n_:x1\n_:x2\n!end\n"),
                arguments(results("?x", "_:x1", "_:x2"), "?x\n_:x1\n_:x2\n!end"),
                arguments(results("?x", "_:x1", "_:x2"), "?x\n_:x1\n_:x2\n"),
                arguments(results("?x", "_:x1", "_:x2"), "?x\n_:x1\n_:x2"),

                // 2 columns
                arguments(results("?x", "?y"), "?x\t?y\n!end\n"),
                arguments(results("?x",  "?y",
                                 "_:x1", null,
                                 null,   "_:y2"),
                          "?x\t?y\n_:x1\t\n\t_:y2\n!end\n"),
                arguments(results("?x", "?y", "_:x1", null, null, "_:y2"),
                          "?x\t?y\n_:x1\t\n\t_:y2"),

                // accept !ping/!ping-ack commands
                arguments(results("?x", "_:x1"), "?x\n!ping\n!ping-ack\n_:x1\n!end\n"),

                // !cancelled and !error
                arguments(results("?x", "_:x1").error(FSCancelledException.class),
                          "?x\n_:x1\n!ping\n!cancelled"),
                arguments(results("?x", "_:x1").error(FSServerException.class),
                          "?x\n_:x1\n!error failure\n"),

                // do not allow results after !end
                arguments(results("?x", "_:x1").error(InvalidSparqlResultsException.class),
                          "?x\n_:x1\n!end\n_:x2"),

                // allow turtle literals
                arguments(results("?x", "11", "true", "false", "23.0", "-1.2e+02"),
                          "?x\n11\ntrue\nfalse\n23.0\n-1.2e+02\n!end\n"),

                // always understand canon prefixes
                arguments(results("?x", "rdf:type", "rdf:Property", "\"23\"^^xsd:int"),
                         "?x\nrdf:type\nrdf:Property\n\"23\"^^xsd:int"),

                // understand !prefix command
                arguments(results("?x", "<http://example.org/Alice>", "foaf:knows", "\"23\"^^xsd:short"),
                          """
                          !prefix p33:<http://www.w3.org/2001/XMLSchema#>
                          ?x
                          !prefix p235:<http://example.org/>
                          !prefix p0:<http://xmlns.com/foaf/0.1/>
                          p235:Alice
                          p0:knows
                          "23"^^p33:short
                          !end
                          """)
        );
    }

    @ParameterizedTest @MethodSource
    void test(Results expected, String in) throws Exception {
        WsFrameSender<ByteRope, ByteRope> frameSender = new WsFrameSender<>() {
            @Override public void sendFrame(ByteRope content) { }
            @Override public ByteRope createSink() { return new ByteRope(); }
            @Override public ResultsSender<ByteRope, ByteRope> createSender() {
                return new ResultsSender<>(WsSerializer.create(), new ByteRope()) {
                    @Override public void preTouch() {}
                    @Override public void sendInit(Vars vars, Vars subset, boolean isAsk) {}
                    @Override public void sendSerializedAll(Batch<?> batch) {}
                    @Override public void sendSerialized(Batch<?> batch, int from, int nRows) {}
                    @Override public void sendTrailer() {}
                    @Override public void sendCancel() {}
                    @Override public void sendError(Throwable cause) {}
                };
            }
        };
        ResultsParser.Factory fac;
        if (in.contains("!active-binding") || in.contains("!bind-request")) {
            fac = new ResultsParser.Factory() {
                @Override public SparqlResultFormat name() { return SparqlResultFormat.WS; }
                @Override
                public <B extends Batch<B>> ResultsParser<B> create(CompletableBatchQueue<B> d) {
                    return new WsClientParser<>(frameSender, d, expected.asBindQuery(d.batchType()), null);
                }
            };
        } else {
            fac = new ResultsParser.Factory() {
                @Override public SparqlResultFormat name() { return SparqlResultFormat.WS; }
                @Override public <B extends Batch<B>> ResultsParser<B> create(CompletableBatchQueue<B> d) {
                    return new WsClientParser<>(frameSender, d);
                }
            };
        }
        int idx = in.indexOf("!end\n");
        if (idx >= 0 && idx < in.length()-5)
            doTestSingleFeed(fac, expected, SegmentRope.of(in));
        else
            doTest(fac, expected, SegmentRope.of(in));
    }
}