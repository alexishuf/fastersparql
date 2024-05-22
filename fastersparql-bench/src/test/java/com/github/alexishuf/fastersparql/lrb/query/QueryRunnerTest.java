package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadCancelledException;
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
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.lrb.query.QueryRunner.Accumulator;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static org.junit.jupiter.api.Assertions.*;

class QueryRunnerTest {
    @ParameterizedTest @ValueSource(booleans = {false, true})
    public void testTimeout(boolean emit) {
        try (var accGuard = new Guard<Accumulator<TermBatch>>(this);
             var slow = new AbstractSparqlClient(SparqlEndpoint.parse("http://example.org/sparql")) {
                @SuppressWarnings("unchecked") @Override
                protected <B extends Batch<B>> BIt<B> doQuery(BatchType<B> bt, SparqlQuery sparql) {
                    var it = new SPSCBIt<>(bt, Vars.of("x"));
                    Thread.startVirtualThread(() -> {
                        for (int i = 0; i < 4; i++) {
                            try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                            try {
                                it.offer((Orphan<B>)TermBatch.of(termList(i)));
                            } catch (TerminatedException|CancelledException ignored) {}
                        }
                    });
                    return it;
                }
                @Override
                protected <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
                doEmit(BatchType<B> bt, SparqlQuery sparql, Vars rebindHint) {
                    return BItEmitter.create(doQuery(bt, sparql));
                }
                @Override public Guard retain() { return NoOpGuard.INSTANCE; }
                @Override protected void doClose() {}
            }) {
            OpaqueSparqlQuery query = new OpaqueSparqlQuery("SELECT * WHERE {?s ?p ?o}");
            var acc = accGuard.set(Accumulator.create(TERM));
            long start = System.nanoTime();
            if (emit)
                QueryRunner.drain(slow.emit(TERM, query, Vars.EMPTY), acc, 1_000);
            else
                QueryRunner.drain(slow.query(TERM, query), acc, 1_000);

            long ms = (System.nanoTime()-start)/1_000_000;
            assertTrue(ms > 900, "run too fast");
            assertTrue(ms < 1_100, "run to slow");
            assertEquals(TermBatch.of(termList(0), termList(1), termList(2), termList(3)),
                         acc.get());
            if (emit)
                assertInstanceOf(FSCancelledException.class, acc.error());
            else
                assertInstanceOf(BItReadCancelledException.class, acc.error());
        }
    }

}