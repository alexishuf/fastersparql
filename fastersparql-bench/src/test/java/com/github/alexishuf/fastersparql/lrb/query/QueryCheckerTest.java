package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class QueryCheckerTest {
    private enum Variant {
        NONE,
        SKIP_LAST,
        EMIT_NULL
    }

    static Stream<Arguments> test() {
        List<Arguments> list = new ArrayList<>();
        for (QueryName qry : QueryName.values()) {
            if (qry.expected(Batch.TERM) == null) continue;
            for (var type : List.of(Batch.TERM, Batch.COMPRESSED)) {
                for (Variant variant : Variant.values())
                    list.add(arguments(qry, type, variant));
            }
        }
        return list.stream();
    }

    @SuppressWarnings("unchecked") @ParameterizedTest @MethodSource("test")
    <B extends Batch<B>> void test(QueryName qry, BatchType<B> bt,
                                   Variant variant) throws Exception {
        CompletableFuture<?> finish = new CompletableFuture<>();
        B ex = qry.expected(bt);
        assertNotNull(ex);
        var consumer = new QueryChecker<>(bt, qry) {
            @Override public void doFinish(@Nullable Throwable error) {
                if (error == null ) finish.complete(null);
                else                finish.completeExceptionally(error);
            }
        };
        var it = new SPSCBIt<>(bt, qry.parsed().publicVars(), FSProperties.queueMaxRows());
        Thread.startVirtualThread(() -> {
            try {
                assertNotNull(ex);
                B tmp = bt.createSingleton(ex.cols);
                int start = ex.rows - (variant == Variant.SKIP_LAST ? 2 : 1);
                for (int r = start; r >= 0; r--) {
                    if (tmp == null && (tmp = it.stealRecycled()) == null)
                        tmp = bt.createSingleton(ex.cols);
                    else
                        tmp.clear();
                    tmp.putRow(ex, r);
                    tmp = it.offer(tmp);
                }
                if (variant == Variant.EMIT_NULL) {
                    if (tmp == null && (tmp = it.stealRecycled()) == null)
                        tmp = bt.createSingleton(ex.cols);
                    else tmp.clear(ex.cols);
                    tmp.beginPut();
                    tmp.commitPut();
                    it.offer(tmp);
                }
                it.complete(null);
            } catch (Throwable t) {
                it.complete(t);
            }
        });
        QueryRunner.drain(it, consumer);
        assertNull(finish.get());
        if (variant == Variant.NONE || (qry == QueryName.C8 && variant == Variant.SKIP_LAST)) {
            assertTrue(consumer.isValid());
        } else {
            assertFalse(consumer.isValid());
            if (variant == Variant.SKIP_LAST) {
                consumer.forEachMissing((b, r) -> {
                    assertTrue(ex.equals(ex.rows-1, (B)b, r));
                    return true;
                });
                assertEquals(0, consumer.unexpected.rows);
            } else if (variant == Variant.EMIT_NULL) {
                assertEquals(1, consumer.unexpected.rows);
                for (int c = 0; c < consumer.unexpected.cols; c++)
                    assertNull(consumer.unexpected.get(0, c));
                boolean[] had = {false};
                consumer.forEachMissing((b, r) -> had[0] = true);
                assertFalse(had[0]);
            }
        }
    }

}