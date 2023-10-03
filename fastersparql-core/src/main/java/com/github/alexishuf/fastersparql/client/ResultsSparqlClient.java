package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.emit.AbstractStage;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.async.TaskEmitter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.stages.BindingStage;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.sparql.results.WsBindingSeq;
import com.github.alexishuf.fastersparql.util.Results;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.*;

import static com.github.alexishuf.fastersparql.FSProperties.queueMaxRows;
import static com.github.alexishuf.fastersparql.batch.type.Batch.*;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.util.Results.*;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;

/**
 * A {@link SparqlClient} that answers queries with predefined {@link Results}s.
 */
public class ResultsSparqlClient extends AbstractSparqlClient {
    private final Map<SparqlQuery, Results> qry2results;
    private final Map<SparqlQuery, Results> qry2WsResults;
    private final Map<SparqlQuery, Results> qry2emitResults;
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
        this.qry2emitResults = new HashMap<>();
        this.qry2err = new HashMap<>();
        this.errors = new ArrayList<>();
    }

    public ResultsSparqlClient(boolean emulateWs, SparqlEndpoint ep,
                               Map<SparqlQuery, Results> qry2results,
                               Map<SparqlQuery, Results> qry2WsResults,
                               Map<SparqlQuery, Results> qry2emitResults,
                               Map<SparqlQuery, RuntimeException> qry2err,
                               List<Throwable> errors) {
        super(ep);
        this.bindingAwareProtocol = emulateWs;
        this.emulateWs = emulateWs;
        this.qry2results = qry2results;
        this.qry2WsResults = qry2WsResults;
        this.qry2emitResults = qry2emitResults;
        this.qry2err = qry2err;
        this.errors = errors;
    }

    @Override public Guard retain() { return NoOpGuard.INSTANCE; }

    @Override protected void doClose() {
        assertNoErrors();
    }

    public ResultsSparqlClient asEmulatingWs() {
        return new ResultsSparqlClient(true, endpoint, qry2results, qry2WsResults, qry2emitResults,
                                       qry2err, errors);
    }

    public @This ResultsSparqlClient answerWith(SparqlQuery query, Results results) {
        Plan parsed = SparqlParser.parse(query);
        qry2results.put(parsed, results);
        qry2emitResults.put(parsed, results);
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
        private final List<List<Term>> rebindRows = new ArrayList<>();

        public BoundAnswersStage1(SparqlQuery unboundQuery, Vars bindingsVars, Vars unboundVars) {
            this.unboundQuery = unboundQuery;
            this.bindingsVars = bindingsVars;
            this.unboundVars = unboundVars;
        }

        public @This BoundAnswersStage2 answer(CharSequence... binding) {
            if (binding.length != bindingsVars.size())
                throw new IllegalArgumentException("Expected "+bindingsVars.size()+" terms");
            var rows = results(bindingsVars, (Object[]) binding).expected();
            if (rows.isEmpty())
                rows = List.of(List.of());
            if (rows.size() != 1)
                throw new IllegalArgumentException("Expected 1 binding, got "+rows.size());
            var bound = unboundQuery.bound(new ArrayBinding(bindingsVars, rows.get(0)));
            return new BoundAnswersStage2(this, bound, rows.get(0));
        }

        public ResultsSparqlClient end() {
            var wsResults = results(Vars.of(WsBindingSeq.VAR).union(unboundVars), wsRows)
                    .bindings(Vars.of(WsBindingSeq.VAR).union(bindingsVars), wsBindings);
            Plan parsed = SparqlParser.parse(unboundQuery);
            qry2WsResults.put(parsed, wsResults);
            qry2WsResults.put(parsed.toAsk(), wsResults);
            Results rebindResults = results(unboundQuery.publicVars(), rebindRows);
            qry2emitResults.put(parsed, rebindResults);
            qry2emitResults.put(parsed.toAsk(), rebindResults);
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
            var results = results(stage1.unboundVars, terms);
            answerWith(boundQuery, results);
            answerWith(boundQuery.toAsk(), results.isEmpty() ? negativeResult() : positiveResult());
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

            for (List<Term> unboundRow : results.expected()) {
                List<Term> rr = new ArrayList<>();
                for (var v : stage1.unboundQuery.publicVars()) {
                    int i = stage1.bindingsVars.indexOf(v);
                    if      ( i                                  >= 0) rr.add(bindingRow.get(i));
                    else if ((i = stage1.unboundVars.indexOf(v)) >= 0) rr.add(unboundRow.get(i));
                    else    throw new AssertionError("Unexpected var " + v);
                }
                stage1.rebindRows.add(rr);
            }
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

    @Override protected <B extends Batch<B>> BIt<B> doQuery(BatchType<B> batchType, SparqlQuery sparql) {
        Results expected = expectedFor(sparql);
        if (expected.hasBindings())
            throw new AssertionError("Expected bindings");
        return batchType.convert(expected.asBIt());
    }

    private Results expectedFor(SparqlQuery sparql) {
        sparql = SparqlParser.parse(sparql);
        Results expected = qry2results.get(sparql);
        if (expected == null) {
            RuntimeException err = qry2err.get(sparql);
            if (err == null)
                throw new AssertionError("Unexpected query: "+ sparql);
            throw err;
        }
        return expected;
    }

    @Override
    protected <B extends Batch<B>> Emitter<B> doEmit(BatchType<B> bt, SparqlQuery sparql,
                                                     Vars rebindHint) {
        return bt.convert(new ResultsEmitter(SparqlParser.parse(sparql)));
    }

    private class ResultsEmitter extends TaskEmitter<TermBatch> {
        private final Plan parsedQuery;
        private final Results expected;
        private @Nullable TermBatch batch;
        private @Nullable TermBatch recycled;
        private final List<List<Term>> actualBindings = new ArrayList<>();

        public ResultsEmitter(Plan parsedQuery) {
            super(Batch.TERM, parsedQuery.publicVars(), EMITTER_SVC, RR_WORKER,
                  CREATED, TASK_EMITTER_FLAGS);
            this.parsedQuery = parsedQuery;
            this.expected = qry2emitResults.getOrDefault(parsedQuery, null);
            if (ResultJournal.ENABLED)
                ResultJournal.initEmitter(this, vars);
            setup(BatchBinding.ofEmpty(TERM));
        }

        @Override protected void doRelease() {
            super.doRelease();
            batch    = TERM.recycle(batch);
            recycled = recyclePooled(recycled);
            if (expected.hasBindings()) {
                List<List<Term>> exBindingRows = expected.bindingsList();
                if (exBindingRows == null)
                    exBindingRows = List.of();
                StringBuilder bindingErrors = new StringBuilder();
                for (List<Term> ac : actualBindings) {
                    if (!exBindingRows.contains(ac))
                        bindingErrors.append("Unexpected binding: ").append(ac);
                }
                for (List<Term> ex : exBindingRows) {
                    if (!actualBindings.contains(ex))
                        bindingErrors.append("No rebind() for expected binding: ").append(ex);
                }
                if (!bindingErrors.isEmpty())
                    errors.add(new AssertionError(bindingErrors.toString()));
            }
        }

        private void setup(BatchBinding binding) {
            resetForRebind(0, 0);
            if (expected == null) {
                this.error = new AssertionError("unexpected query: "+parsedQuery);
                return;
            }
            Vars allVars = expected.vars();
            var batch = this.batch;
            this.batch = batch = Batch.TERM.empty(batch, 1, allVars.size(), 0);
            List<List<Term>> rows = expected.expected();
            outer:
            for (var row : rows) {
                for (int bIdx = 0; bIdx < binding.vars.size(); bIdx++) {
                    int i = allVars.indexOf(binding.vars.get(bIdx)) ;
                    if (i >= 0 && !Objects.equals(row.get(i), binding.get(bIdx)))
                        continue outer; // rows does not match binding
                }
                batch.putRow(row); // row matches binding
            }
            Boolean askResult = switch (expected.bindType()) {
                case JOIN,LEFT_JOIN   -> null;
                case EXISTS           -> batch.rows > 0;
                case NOT_EXISTS,MINUS -> batch.rows == 0;
            };
            if (askResult != null) {
                batch.clear(0);
                batch.rows = askResult ? 1 : 0;
            }
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            if (EmitterStats.ENABLED && stats != null)
                stats.onRebind(binding);
            if (ResultJournal.ENABLED)
                ResultJournal.rebindEmitter(this, binding);
            List<Term> actualBinding = new ArrayList<>();
            Vars exBindingVars = expected.bindingsVars();
            for (SegmentRope var : (exBindingVars == null ? binding.vars : exBindingVars))
                actualBinding.add(binding.get(var));
            actualBindings.add(actualBinding);
            setup(binding);
        }

        @Override protected int produceAndDeliver(int state) {
            if (batch != null)
                recycled = asPooled(deliver(batch.copy(asUnpooled(recycled))));
            return error == UNSET_ERROR ? COMPLETED : FAILED;
        }
    }

    @Override protected <B extends Batch<B>> BIt<B> doQuery(ItBindQuery<B> bq) {
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

    @Override protected <B extends Batch<B>> Emitter<B> doEmit(EmitBindQuery<B> bq,
                                                               Vars rebindHint) {
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
            var bindingsChecker = exBindings.checker(bq.bindings);
            var bt = bq.bindings.batchType();
            AbstractStage<TermBatch, TermBatch> intercepted = new AbstractStage<>(Batch.TERM, expected.vars()) {
                private volatile boolean started = false;
                private volatile boolean failed = false;

                @Override public @Nullable TermBatch onBatch(TermBatch batch) {
                    return downstream.onBatch(batch);
                }

                @Override public void onRow(TermBatch batch, int row) {
                    downstream.onRow(batch, row);
                }

                @Override public String toString() { return "CheckBindings<-"+upstream; }

                @Override public void request(long rows) {
                    if (!started) {
                        started = true;
                        try {
                            bindingsChecker.assertNoError();
                        } catch (Throwable t) {
                            failed = true;
                            upstream.cancel();
                            downstream.onError(t);
                        }
                    }
                    super.request(rows);
                }
                @Override public void onComplete()         { if (!failed) super.onComplete(); }
                @Override public void onCancelled()        { if (!failed) super.onCancelled(); }
                @Override public void onError(Throwable e) { if (!failed) super.onError(e); }
            };
            intercepted.subscribeTo(expected.asEmitter());
            return bt.convert(intercepted);
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
            return new BindingStage.ForSparql<>(bq, Vars.EMPTY, this);
        }
    }

    private AssertionError error(String msg) {
        var e = new AssertionError(msg);
        errors.add(e);
        return e;
    }
}
