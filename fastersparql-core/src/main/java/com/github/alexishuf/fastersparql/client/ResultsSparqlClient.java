package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.sparql.results.WsBindingSeq;
import com.github.alexishuf.fastersparql.util.Results;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.alexishuf.fastersparql.FSProperties.queueMaxRows;
import static com.github.alexishuf.fastersparql.util.Results.negativeResult;
import static com.github.alexishuf.fastersparql.util.Results.results;

/**
 * A {@link SparqlClient} that answers queries with predefined {@link Results}s.
 */
public class ResultsSparqlClient extends AbstractSparqlClient {
    private final Map<SparqlQuery, Results> qry2results;
    private final Map<SparqlQuery, Results> qry2WsResults;
    private final Map<SparqlQuery, RuntimeException> qry2err;
    private final List<Throwable> errors;
    private final boolean emulateWs;

    public ResultsSparqlClient(boolean nativeBind) {
        this(nativeBind, "http://example.org/sparql");
    }

    public ResultsSparqlClient(boolean nativeBind, String endpoint) {
        this(nativeBind, SparqlEndpoint.parse(endpoint));
    }

    public ResultsSparqlClient(boolean nativeBind, SparqlEndpoint endpoint) {
        super(endpoint);
        this.emulateWs = false;
        this.bindingAwareProtocol = nativeBind;
        this.qry2results = new HashMap<>();
        this.qry2WsResults = new HashMap<>();
        this.qry2err = new HashMap<>();
        this.errors = new ArrayList<>();
    }

    public ResultsSparqlClient(boolean emulateWs, SparqlEndpoint ep,
                               Map<SparqlQuery, Results> qry2results,
                               Map<SparqlQuery, Results> qry2WsResults,
                               Map<SparqlQuery, RuntimeException> qry2err,
                               List<Throwable> errors) {
        super(ep);
        this.bindingAwareProtocol = emulateWs;
        this.emulateWs = emulateWs;
        this.qry2results = qry2results;
        this.qry2WsResults = qry2WsResults;
        this.qry2err = qry2err;
        this.errors = errors;
    }

    @Override public Guard retain() { return NoOpGuard.INSTANCE; }

    @Override protected void doClose() {}

    public ResultsSparqlClient asEmulatingWs() {
        return new ResultsSparqlClient(true, endpoint, qry2results, qry2WsResults,
                                       qry2err, errors);
    }

    public @This ResultsSparqlClient answerWith(SparqlQuery query, Results results) {
        qry2results.put(SparqlParser.parse(query), results);
        return this;
    }

    public @This ResultsSparqlClient answerWith(SparqlQuery query, RuntimeException t) {
        qry2err.put(SparqlParser.parse(query), t);
        return this;
    }

    public class BoundAnswersStage1 {
        private final SparqlQuery unboundQuery;
        private final Vars bindingsVars;
        private final Vars unboundVars;
        private int bindingSeq = 0;
        private final List<List<Term>> wsRows = new ArrayList<>();
        private final List<List<Term>> wsBindings = new ArrayList<>();

        public BoundAnswersStage1(SparqlQuery unboundQuery, Vars bindingsVars, Vars unboundVars) {
            this.unboundQuery = unboundQuery;
            this.bindingsVars = bindingsVars;
            this.unboundVars = unboundVars;
        }

        public @This BoundAnswersStage2 answer(CharSequence... binding) {
            if (binding.length != bindingsVars.size())
                throw new IllegalArgumentException("Expected "+bindingsVars.size()+" terms");
            var rows = results(bindingsVars, (Object[]) binding).expected();
            if (rows.size() == 0)
                rows = List.of(List.of());
            if (rows.size() != 1)
                throw new IllegalArgumentException("Expected 1 binding, got "+rows.size());
            var bound = unboundQuery.bound(new ArrayBinding(bindingsVars, rows.get(0)));
            return new BoundAnswersStage2(this, bound, rows.get(0));
        }

        public ResultsSparqlClient end() {
            var wsResults = results(Vars.of(WsBindingSeq.VAR).union(unboundVars), wsRows)
                    .bindings(Vars.of(WsBindingSeq.VAR).union(bindingsVars), wsBindings);
            qry2WsResults.put(unboundQuery, wsResults);
            return ResultsSparqlClient.this;
        }
    }

    public class BoundAnswersStage2 {
        private final BoundAnswersStage1 stage1;
        private final SparqlQuery boundQuery;
        private final List<Term> bindingRow;

        public BoundAnswersStage2(BoundAnswersStage1 stage1, SparqlQuery boundQuery,
                                  List<Term> bindingRow) {
            this.stage1 = stage1;
            this.boundQuery = boundQuery;
            this.bindingRow = bindingRow;
        }

        public BoundAnswersStage1 with(Object... terms) {
            Results results = results(stage1.unboundVars, terms);
            //noinspection resource
            answerWith(boundQuery, results);
            Term seq = new WsBindingSeq().toTerm(stage1.bindingSeq);
            for (List<Term> row : results.expected()) {
                ArrayList<Term> withSeq = new ArrayList<>(row.size() + 1);
                withSeq.add(seq);
                withSeq.addAll(row);
                stage1.wsRows.add(withSeq);
            }
            List<Term> bindingWithSeq = new ArrayList<>(bindingRow.size()+1);
            bindingWithSeq.add(seq);
            bindingWithSeq.addAll(bindingRow);
            stage1.wsBindings.add(bindingWithSeq);
            ++stage1.bindingSeq;
            return stage1;
        }
    }

    public BoundAnswersStage1 forBindings(SparqlQuery unboundQuery, Vars bindingsVars,
                                          Vars unboundVars) {
        return new BoundAnswersStage1(unboundQuery, bindingsVars, unboundVars);
    }

    public void assertNoErrors() {
        if (!errors.isEmpty()) {
            String msg = "There were errors:"
                       + errors.stream().map(e -> "  " + e + "\n").reduce(String::concat).orElse("");
            throw new AssertionError(msg);
        }
    }

    @Override public <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql) {
        sparql = SparqlParser.parse(sparql);
        Results expected = qry2results.get(sparql);
        if (expected == null) {
            RuntimeException err = qry2err.get(sparql);
            if (err == null)
                throw new AssertionError("Unexpected query: "+sparql);
            throw err;
        }
        if (expected.hasBindings())
            throw new AssertionError("Expected bindings");
        return batchType.convert(expected.asBIt());
    }

    @Override public <B extends Batch<B>> BIt<B> query(BindQuery<B> bq) {
        var sparql = bq.parsedQuery();
        Results expected = emulateWs ? qry2WsResults.get(sparql) : qry2results.get(sparql);
        if (expected == null) {
            RuntimeException err = qry2err.get(sparql);
            if (err != null)
                throw err;
            throw new AssertionError("Unexpected query " + sparql);
        }
        Results exBindings = expected.hasBindings() ? expected.bindingsAsResults()
                                                    : negativeResult();
        if (!expected.hasBindings()) {
            throw error("Did not expected bindings for "+sparql);
        } else if (bq.type == null) {
            throw new NullPointerException("bindings != null but type == null");
        }
        if (usesBindingAwareProtocol()) {
            var batchType = bq.bindings.batchType();
            var cb = new SPSCBIt<>(batchType, expected.vars(), queueMaxRows());
            Thread.startVirtualThread(() -> {
                Thread.currentThread().setName("feeder-"+endpoint+"-"+cb);
                try {
                    exBindings.check(bq.bindings);
                    try (BIt<B> it = batchType.convert(expected.asBIt())) {
                        for (B b = null; (b = it.nextBatch(b)) != null; )
                            b = cb.offer(b);
                    } finally {
                        cb.complete(null);
                    }
                } catch (Throwable t) {
                    cb.complete(t);
                }
            });
            return cb;
        } else {
            Vars unboundVars = expected.vars().minus(bq.bindings.vars());
            for (List<Term> bindingRow : exBindings.expected()) {
                var bound = sparql.bound(new ArrayBinding(exBindings.vars(), bindingRow));
                Results boundExpected = qry2results.get(bound);
                if (boundExpected == null)
                    throw error("No result defined for bound query "+bound);
                if (boundExpected.hasBindings())
                    throw error("Bound query results expect bindings: "+bound);
                if (!boundExpected.vars().equals(unboundVars))
                    throw error("Bound query results vars do not match ");
            }
            return new ClientBindingBIt<>(bq, this);
        }
    }

    private AssertionError error(String msg) {
        var e = new AssertionError(msg);
        errors.add(e);
        return e;
    }
}
