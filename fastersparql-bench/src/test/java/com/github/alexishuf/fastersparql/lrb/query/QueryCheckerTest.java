package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.lrb.query.QueryName.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class QueryCheckerTest {
    private static final Logger log = LoggerFactory.getLogger(QueryCheckerTest.class);

    private enum Variant {
        NONE,
        SKIP_LAST,
        EMIT_NULL
    }

    static Stream<Arguments> test() {
        List<Arguments> list = new ArrayList<>();
        for (QueryName qry : QueryName.values()) {
            if (qry.expected(TermBatchType.TERM) == null) continue;
            for (var type : List.of(TermBatchType.TERM, CompressedBatchType.COMPRESSED)) {
                for (Variant variant : Variant.values())
                    list.add(arguments(qry, type, variant));
            }
        }
        return list.stream();
    }

    @SuppressWarnings("unchecked") @ParameterizedTest @MethodSource("test")
    <B extends Batch<B>> void test(QueryName qry, BatchType<B> bt,
                                   Variant variant) throws Exception {
        CompletableFuture<Throwable> finish = new CompletableFuture<>();
        B ex = qry.expected(bt);
        assertNotNull(ex);
        int exRows = ex.totalRows();
        var consumer = new QueryChecker<>(bt, qry) {
            @Override public void doFinish(@Nullable Throwable error) {
                boolean expectValid = variant == Variant.NONE;
                if (variant == Variant.SKIP_LAST && (qry == C7 || qry == C8 || qry == C10))
                    expectValid = true; // last 2 rows equal after amputateNumbers()
                try {
                    if (error == null && !isValid()) {
                        if (expectValid) {
                            String explanation = explanation();
                            log.error("Bad results for {}: {}", qry, explanation);
                            fail("Bad results: "+explanation.replace("\n", "\\n"));
                        } else if (variant == Variant.SKIP_LAST) {
                            forEachMissing((b, r) -> {
                                assertTrue(ex.linkedEquals(exRows-1, (B) b, r));
                                return true;
                            });
                            assertEquals(0, unexpected.rows);
                        } else if (variant == Variant.EMIT_NULL) {
                            assertEquals(1, unexpected.rows);
                            for (int c = 0; c < unexpected.cols; c++)
                                assertNull(unexpected.get(0, c));
                            boolean[] had = {false};
                            forEachMissing((b, r) -> {
                                had[0] = true;
                                return false;
                            });
                            assertFalse(had[0]);
                        }
                    } else if (error == null && !expectValid) {
                        fail("Expected !isValid(), variant="+variant);
                    }
                } catch (Throwable t) {
                    error = t;
                }
                if (error == null ) finish.complete(null);
                else                finish.completeExceptionally(error);
            }
        };
        var it = new SPSCBIt<>(bt, qry.parsed().publicVars());
        Thread.startVirtualThread(() -> {
            try {
                assertNotNull(ex);
                B tmp = bt.create(ex.cols);
                int absRow = 0;
                for (var node = ex; node != null; node = node.next) {
                    int start = node.rows-1;
                    if (variant == Variant.SKIP_LAST && absRow+node.rows == exRows)
                        --start;
                    for (int r = start; r >= 0; r--) {
                        tmp = bt.empty(tmp, node.cols);
                        tmp.putRow(node, r);
                        tmp = it.offer(tmp);
                    }
                    if (variant == Variant.EMIT_NULL && absRow+node.rows == exRows) {
                        tmp = bt.empty(tmp, node.cols);
                        tmp.beginPut();
                        tmp.commitPut();
                        bt.recycle(tmp = it.offer(tmp));
                    }
                    absRow += node.rows;
                }
                it.complete(null);
            } catch (Throwable t) {
                it.complete(t);
            }
        });
        QueryRunner.drain(it, consumer);
        Throwable error = finish.get();
        if (error != null)
            fail(error);
    }

}