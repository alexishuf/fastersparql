package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.results.ResultsParserBIt;
import com.github.alexishuf.fastersparql.sparql.results.ResultsSender;
import com.github.alexishuf.fastersparql.sparql.results.WsClientParserBIt;
import com.github.alexishuf.fastersparql.sparql.results.WsFrameSender;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.sparql.results.serializer.WsSerializer;
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
        assertTrue(plan.publicVars().size() > 0);
        assertTrue(plan.allVars().size() > 0);
        assertEquals(plan, name.parsed());
    }

    static Stream<Arguments> testParseResults() {
        List<Arguments> list = new ArrayList<>();
        for (QueryName name : QueryName.values()) {
            for (var type : List.of(Batch.TERM, Batch.COMPRESSED))
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
        for (int r = 0; r < expected.rows; r++) {
            boolean allNull = true;
            for (int c = 0; allNull && c < expected.cols; c++)
                allNull = expected.termType(r, c) == null;
            assertFalse(allNull, "r="+r);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"S1", "S2", "S7", "S9", "S10", "S11", "S12", "S14",
                            "C2", "C8", "C7"})
    public void testHasResults(String nameString) {
        var name = QueryName.valueOf(nameString);
        for (var type : List.of(Batch.TERM, Batch.COMPRESSED)) {
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
        serializer.serialize(expected, sink);
        serializer.serializeTrailer(sink);

        //parse
        try (var parse = createParser(format, type, vars)) {
            parse.feedShared(sink);
            parse.complete(null);
            B acc = type.create(expected.rows, expected.cols, expected.bytesUsed());
            for (B b = null; (b = parse.nextBatch(b)) != null; )
                acc.put(b);
            assertEquals(expected, acc);
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

    private <B extends Batch<B>, S extends ByteSink<S, T>, T> ResultsParserBIt<B>
    createParser(SparqlResultFormat format, BatchType<B> type, Vars vars) {
        if (format == WS) {
            WsFrameSender<?, ?> frameSender = new WsFrameSender<S, T>() {
                @Override public void sendFrame(T content) {}
                @Override public S createSink() {return null;}
                @Override public ResultsSender<S, T> createSender() {
                    return new ResultsSender<>(WsSerializer.create(), createSink()) {
                        @Override public void sendInit(Vars vars, Vars subset, boolean isAsk) {
                            serializer.init(vars, subset, isAsk, new ByteRope());
                        }
                        @Override public void sendSerialized(Batch<?> batch) {
                            serializer.serialize(batch, new ByteRope());
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
            return new WsClientParserBIt<>(frameSender, type, vars, 1<<16);
        } else {
            return ResultsParserBIt.createFor(format, type, vars, 1<<16);
        }
    }
}