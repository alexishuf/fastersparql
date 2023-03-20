package com.github.alexishuf.fastersparql.sparql.results;


import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.rope.BufferRope;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.List;

import static com.github.alexishuf.fastersparql.FSProperties.queueMaxBatches;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResultsParserTest {
    private static final int REPETITIONS = Runtime.getRuntime().availableProcessors();

    interface RopeFac {
        Rope create(Rope src, int begin, int end);
        void invalidate(Rope r);
    }

    private static class ByteRopeFac implements RopeFac {
        @Override public Rope create(Rope src, int begin, int end) {
            return new ByteRope(end-begin).append(src, begin, end);
        }
        @Override public void invalidate(Rope r) { ((ByteRope)r).fill('\n'); }
    }

    private static class OffsetByteRopeFac implements RopeFac {
        @Override public Rope create(Rope src, int begin, int end) {
            byte[] u8 = new byte[end - begin + 2];
            src.copy(begin, end, u8, 2);
            return new ByteRope(u8, 2, end-begin);
        }

        @Override public void invalidate(Rope r) {
            byte[] u8 = ((ByteRope) r).utf8;
            for (int i = 0; i < u8.length; i++) {
                u8[i] = switch (i&3) {
                    case 0  -> '\t';
                    case 1  -> ',';
                    case 2  -> '\r';
                    default -> '\n';
                };
            }
        }
    }

    private static class BufferRopeFac implements RopeFac {
        @Override public Rope create(Rope src, int begin, int end) {
            byte[] u8 = new byte[6 + end - begin];
            u8[0] = '\t';
            u8[1] = '\r';
            u8[2] = '\n';
            src.copy(begin, end, u8, 3);
            u8[u8.length-1] = '\n';
            u8[u8.length-2] = '\r';
            u8[u8.length-3] = ',';
            return new BufferRope(ByteBuffer.wrap(u8).position(3).limit(3+end-begin));
        }

        @Override public void invalidate(Rope r) {
            var buffer = ((BufferRope) r).buffer;
            byte[] filled = new byte[r.len() + 6];
            for (int i = 0; i < filled.length; i++) {
                filled[i] = switch (i&3) {
                    case 0  -> '\r';
                    case 1  -> '\n';
                    case 2  -> ',';
                    default -> '\t';
                };
            }
            int limit = buffer.limit();
            buffer.limit(filled.length).put(0, filled);
            buffer.limit(limit);
        }
    }

    private static final List<RopeFac> ROPE_FACTORIES = List.of(
            new ByteRopeFac(),
            new BufferRopeFac(),
            new OffsetByteRopeFac()
    );

    protected void doTestSingleFeed(ResultsParserBIt.Factory factory, Results expected,
                                    Rope input) throws Exception {
        for (RopeFac ropeFac : ROPE_FACTORIES)
            singleFeed(factory, expected, input, ropeFac);
        try (var tasks = TestTaskSet.virtualTaskSet(getClass().getSimpleName())) {
            for (RopeFac ropeFac : ROPE_FACTORIES)
                tasks.repeat(REPETITIONS, () -> singleFeed(factory, expected, input, ropeFac));
        }
    }

    protected void doTest(ResultsParserBIt.Factory factory, Results expected,
                          Rope input) throws Exception {
        for (RopeFac ropeFac : ROPE_FACTORIES) {
            singleFeed(factory, expected, input, ropeFac);
            byteFeed(factory, expected, input, ropeFac);
            wsFeed(factory, expected, input, ropeFac);
            lineFeed(factory, expected, input, ropeFac);
        }
        try (var tasks = TestTaskSet.virtualTaskSet(getClass().getSimpleName())) {
            for (RopeFac ropeFac : ROPE_FACTORIES) {
                tasks.repeat(REPETITIONS, () -> singleFeed(factory, expected, input, ropeFac));
                tasks.repeat(REPETITIONS, () -> byteFeed(factory, expected, input, ropeFac));
                tasks.repeat(REPETITIONS, () -> wsFeed(factory, expected, input, ropeFac));
                tasks.repeat(REPETITIONS, () -> lineFeed(factory, expected, input, ropeFac));
            }
        }
    }

    private void singleFeed(ResultsParserBIt.Factory factory, Results ex, Rope input, RopeFac ropeFac) {
        try (var dst = new SPSCBIt<>(Batch.TERM, ex.vars(), queueMaxBatches());
             var parser = factory.create(Batch.TERM, dst)) {
            Rope copy = ropeFac.create(input, 0, input.len());
            try {
                parser.feedShared(input);
                ropeFac.invalidate(copy);
            } catch (Throwable ignored) {
            } finally {
                if (!parser.isCompleted())
                    parser.complete(null);
            }
            ex.check(dst);
        }
    }

    private void byteFeed(ResultsParserBIt.Factory factory, Results ex, Rope input, RopeFac ropeFac) {
        try (var dst = new SPSCBIt<>(Batch.TERM, ex.vars(), queueMaxBatches());
             var parser = factory.create(Batch.TERM, dst)) {
            Thread.startVirtualThread(() -> {
                try {
                    for (int i = 0, len = input.len(); i < len; i++) {
                        Rope r = ropeFac.create(input, i, i + 1);
                        parser.feedShared(r);
                        ropeFac.invalidate(r);
                    }
                } catch (Throwable ignored) {
                } finally {
                    if (!parser.isFailed())
                        parser.complete(null);
                }
            });
            ex.check(dst);
        }
    }

    private void wsFeed(ResultsParserBIt.Factory factory, Results ex, Rope input,
                        RopeFac ropeFac) {
        try (var parser = factory.create(Batch.TERM, ex.vars(), queueMaxBatches())) {
            Thread.startVirtualThread(() -> {
                try {
                    for (int i = 0, j, len = input.len(); i < len; i = j) {
                        j = input.skip(i + 1, len, Rope.UNTIL_WS);
                        Rope r = ropeFac.create(input, i, j);
                        parser.feedShared(r);
                        ropeFac.invalidate(r);
                    }
                } catch (Throwable ignored) {
                } finally {
                    if (!parser.isCompleted())
                        parser.complete(null);
                }
            });
            ex.check(parser);
        }
    }

    private void lineFeed(ResultsParserBIt.Factory factory, Results ex, Rope input,
                          RopeFac ropeFac) {
        try (var dst = new SPSCBIt<>(Batch.TERM, ex.vars(), queueMaxBatches());
             var parser = factory.create(Batch.TERM, dst)) {
            Thread.startVirtualThread(() -> {
                try {
                    for (int i = 0, j, len = input.len(); i < len; i = j) {
                        j = input.skip(i + 1, len, Rope.UNTIL_WS);
                        Rope r = ropeFac.create(input, i, j);
                        parser.feedShared(r);
                        ropeFac.invalidate(r);
                    }
                } catch (Throwable ignored) {
                } finally {
                    if (!parser.isFailed())
                        parser.complete(null);
                }
            });
            ex.check(dst);
        }
    }

    @ParameterizedTest @ValueSource(strings = {"JSON", "TSV", "CSV"})
    void testSupportsFormat(String fmtName) {
        var fmt = SparqlResultFormat.valueOf(fmtName);
        assertTrue(ResultsParserBIt.supports(fmt));
    }
}