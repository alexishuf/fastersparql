package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.async.BItEmitter;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.github.alexishuf.fastersparql.FSProperties.queueMaxRows;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryRunnerTest {
    @ParameterizedTest @ValueSource(booleans = {false, true})
    public void testTimeout(boolean emit) {
        TermBatch[] acc = {null};
        Throwable[] cause = {null};
        var consumer = new QueryRunner.Accumulator<>(Batch.TERM) {
            @Override public void finish(@Nullable Throwable error) {
               acc[0] = batch;
               cause[0] = error;
            }
        };
        var slow = new AbstractSparqlClient(SparqlEndpoint.parse("http://example.org/sparql")) {
            @SuppressWarnings("unchecked") @Override
            protected <B extends Batch<B>> BIt<B> doQuery(BatchType<B> bt, SparqlQuery sparql) {
                var it = new SPSCBIt<>(bt, Vars.of("x"), queueMaxRows());
                Thread.startVirtualThread(() -> {
                    for (int i = 0; i < 4; i++) {
                        try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                        try {
                            it.offer((B) TermBatch.of(termList(i)));
                        } catch (TerminatedException|CancelledException ignored) {}
                    }
                });
               return it;
            }
            @Override
            protected <B extends Batch<B>> Emitter<B> doEmit(BatchType<B> bt, SparqlQuery sparaql) {
                return new BItEmitter<>(doQuery(bt, sparaql));
            }
            @Override public Guard retain() { return NoOpGuard.INSTANCE; }
            @Override protected void doClose() {}
        };
        OpaqueSparqlQuery query = new OpaqueSparqlQuery("SELECT * WHERE {?s ?p ?o}");
        long start = System.nanoTime();
        if (emit) {
            QueryRunner.drain(slow.emit(consumer.batchType(), query), consumer, 1_000);
        } else {
            QueryRunner.drain(slow.query(consumer.batchType(), query), consumer, 1_000);
        }
        long ms = (System.nanoTime()-start)/1_000_000;
        assertTrue(ms > 900, "run too fast");
        assertTrue(ms < 1_100, "run to slow");
        assertEquals(TermBatch.of(termList(0), termList(1), termList(2), termList(3)), acc[0]);
        assertTrue(cause[0] instanceof BItReadClosedException);
        slow.close();
    }

}