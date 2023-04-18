package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import static com.github.alexishuf.fastersparql.FSProperties.queueMaxRows;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryRunnerTest {
    @Test public void testTimeout() {
        TermBatch[] acc = {null};
        Throwable[] cause = {null};
        var consumer = new QueryRunner.Accumulator<>(Batch.TERM) {
            @Override public void finish(@Nullable Throwable error) {
               acc[0] = batch;
               cause[0] = error;
            }
        };
        AbstractSparqlClient slow = new AbstractSparqlClient(SparqlEndpoint.parse("http://example.org/sparql")) {
            @SuppressWarnings("unchecked") @Override
            public <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql) {
                var it = new SPSCBIt<>(batchType, Vars.of("x"), queueMaxRows());
                Thread.startVirtualThread(() -> {
                    for (int i = 0; i < 4; i++) {
                        try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                        it.offer((B) TermBatch.of(termList(i)));
                    }
                });
               return it;
            }
            @Override public void close() {}
        };
        OpaqueSparqlQuery query = new OpaqueSparqlQuery("SELECT * WHERE {?s ?p ?o}");
        long start = System.nanoTime();
        QueryRunner.run(slow, query, consumer, 1_000);
        long ms = (System.nanoTime()-start)/1_000_000;
        assertTrue(ms > 900, "run too fast");
        assertTrue(ms < 1_100, "run to slow");
        assertEquals(TermBatch.of(termList(0), termList(1), termList(2), termList(3)), acc[0]);
        assertTrue(cause[0] instanceof BItReadClosedException);
    }

}