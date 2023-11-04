package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.results.ResultsParser;
import com.github.alexishuf.fastersparql.sparql.results.ResultsSender;
import com.github.alexishuf.fastersparql.sparql.results.WsClientParser;
import com.github.alexishuf.fastersparql.sparql.results.WsFrameSender;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.sparql.results.serializer.WsSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class QueryNameTest {

    @ParameterizedTest @EnumSource(QueryName.class)
    public void testOpaqueQuery(QueryName name) {
        OpaqueSparqlQuery q = name.opaque();
        assertNotNull(q);
        assertTrue(q.sparql().len > 0);
        assertFalse(q.publicVars().isEmpty());
    }

    @ParameterizedTest @EnumSource(QueryName.class)
    public void testParseAllQueries(QueryName name) {
        Plan plan = name.parsed();
        assertNotNull(plan);
        assertFalse(plan.publicVars().isEmpty());
        assertFalse(plan.allVars().isEmpty());
        assertEquals(plan, name.parsed());
    }

    static Stream<Arguments> testParseResults() {
        List<Arguments> list = new ArrayList<>();
        for (QueryName name : QueryName.values()) {
            for (var type : List.of(TermBatchType.TERM, CompressedBatchType.COMPRESSED))
                list.add(arguments(name, type));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    public <B extends Batch<B>> void testParseResults(QueryName name, BatchType<B> type) {
        B expected = name.expected(type);
        if (expected == null) return;
        assertTrue(expected.rows > 0);
        assertEquals(name.parsed().publicVars().size(), expected.cols);
        for (B node = expected; node != null; node = node.next) {
            for (int r = 0; r < node.rows; r++) {
                boolean allNull = true;
                for (int c = 0; allNull && c < node.cols; c++)
                    allNull = node.termType(r, c) == null;
                assertFalse(allNull, "r=" + r);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"S1", "S2", "S7", "S9", "S10", "S11", "S12", "S14",
                            "C2", "C8", "C7"})
    public void testHasResults(String nameString) {
        var name = QueryName.valueOf(nameString);
        for (var type : List.of(TermBatchType.TERM, CompressedBatchType.COMPRESSED)) {
            //noinspection unchecked,rawtypes
            Batch<?> expected = name.expected((BatchType) type);
            assertNotNull(expected);
            assertTrue(expected.rows > 0);
        }
    }

    static Stream<Arguments> testParseResultsSerializeAndParse() {
        List<Arguments> list = new ArrayList<>();
        for (var fmt : List.of(TSV, WS, JSON)) {
            testParseResults().map(Arguments::get)
                    .forEach(a -> list.add(arguments(a[0], a[1], fmt)));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    public <B extends Batch<B>>
    void testParseResultsSerializeAndParse(QueryName name, BatchType<B> type,
                                           SparqlResultFormat format) {
        Vars vars = name.parsed().publicVars();
        B expected = name.expected(type);
        if (expected == null)
            return;
//        if (format == WS)
//            expected = prepareExpectedForWS(expected, type);

        // serialize
        var serializer = ResultsSerializer.create(format);
        ByteRope sink = new ByteRope();
        serializer.init(vars, vars, false, sink);
        serializer.serializeAll(expected, sink);
        serializer.serializeTrailer(sink);

        //parse
        try (var parsed = new SPSCBIt<>(type, vars, FSProperties.queueMaxRows())) {
            var parser = createParser(format, parsed);
            parser.feedShared(sink);
            parser.feedEnd();
            B acc = type.create(expected.cols);
            acc.reserveAddLocals(expected.localBytesUsed());
            for (B b = null; (b = parsed.nextBatch(b)) != null; )
                acc.copy(b);
            assertEquals(expected, acc);
        } catch (BatchQueue.CancelledException | BatchQueue.TerminatedException e) {
            throw new RuntimeException("Unexpected "+e.getClass().getSimpleName());
        }
    }

//    private <B extends Batch<B>> B prepareExpectedForWS(B expected, BatchType<B> type) {
//        ByteRope tmp = new ByteRope();
//        B fix = type.create(expected.rows, expected.cols, expected.bytesUsed());
//        for (int r = 0, rows = expected.rows, cols = expected.cols; r < rows; r++) {
//            fix.beginPut();
//            for (int c = 0; c < cols; c++) {
//                if (expected.termType(r, c) == Term.Type.IRI) {
//                    Term term = Objects.requireNonNull(expected.get(r, c));
//                    int len = term.local.length;
//                    byte last = len < 2 ? (byte)'a' : term.local[len - 2];
//                    if (!Rope.contains(PN_LOCAL_LAST, last)) {
//                        tmp.clear().append(term.local, 0, len-2)
//                                .append('\\').append(last);
//                        fix.putTerm(c, term.flaggedDictId, tmp, 0, tmp.len);
//                        continue;
//                    }
//                }
//                fix.putTerm(c, expected, r, c);
//            }
//            fix.commitPut();
//        }
//        return fix;
//    }

    private <B extends Batch<B>, S extends ByteSink<S, T>, T> ResultsParser<B>
    createParser(SparqlResultFormat format, CompletableBatchQueue<B> dest) {
        if (format == WS) {
            WsFrameSender<?, ?> frameSender = new WsFrameSender<S, T>() {
                @Override public void sendFrame(T content) {}
                @Override public S createSink() {return null;}
                @Override public ResultsSender<S, T> createSender() {
                    return new ResultsSender<>(WsSerializer.create(), createSink()) {
                        @Override public void preTouch() {}
                        @Override public void sendInit(Vars vars, Vars subset, boolean isAsk) {
                            serializer.init(vars, subset, isAsk, new ByteRope());
                        }
                        @Override public void sendSerializedAll(Batch<?> batch) {
                            serializer.serializeAll(batch, new ByteRope());
                        }
                        @Override public void sendSerialized(Batch<?> batch, int from, int nRows) {
                            serializer.serialize(batch, from, nRows, new ByteRope());
                        }
                        @Override public void sendTrailer() {
                            serializer.serializeTrailer(new ByteRope());
                        }
                        @Override public void sendError(Throwable cause) {}
                        @Override public void sendCancel() {}
                    };
                }
            };
            return new WsClientParser<>(frameSender, dest);
        } else {
            return ResultsParser.createFor(format, dest);
        }
    }

    @Test void regressionC8() {
        var r0 = Term.termList(
                "<http://data.semanticweb.org/conference/iswc/2009/paper/research/306>",
                "\"Berlin, Heidelberg, New York\"",
                "<http://data.semanticweb.org/person/christian-bizer>",
                "<http://data.semanticweb.org/organization/freie-universitaet-berlin>",
                "\"Christian Bizer\"",
                "<http://dbpedia.org/resource/Germany>",
                "<http://dbpedia.org/resource/Berlin>",
                "\"228.959\"^^xsd:double",
                "<http://dbpedia.org/resource/Federal_republic>",
                "<http://dbpedia.org/resource/German_language>",
                "\"President\"@en"
        );
        var rX = Term.termList(
                "<http://data.semanticweb.org/conference/iswc/2009/paper/poster_demo/158>",
                "\"Berlin, Heidelberg, New York\"",
                "<http://data.semanticweb.org/person/yuan-ren>",
                "<http://data.semanticweb.org/organization/university-of-aberdeen>",
                "\"Yuan Ren\"",
                "<http://dbpedia.org/resource/United_Kingdom>",
                "<http://dbpedia.org/resource/London>",
                "\"254.673\"^^xsd:double",
                "<http://dbpedia.org/resource/Constitutional_monarchy>",
                "<http://dbpedia.org/resource/English_language>",
                "\"Prime Minister\"@en"
        );

        var b = QueryName.C8.expected(CompressedBatchType.COMPRESSED);
        assertNotNull(b);
        var bTail = b.tail();
        assertEquals(r0.size(), b.cols);
        for (int c = 0; c < b.cols; c++) {
            assertEquals(r0.get(c),     b.get(0,            c), "c="+c);
            assertEquals(rX.get(c), bTail.get(bTail.rows-1, c), "c="+c);
        }
    }
}