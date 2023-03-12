package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.adapters.CallbackBIt;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.RowBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.Results;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.alexishuf.fastersparql.model.row.RowType.LIST;
import static com.github.alexishuf.fastersparql.util.Results.negativeResult;
import static com.github.alexishuf.fastersparql.util.Results.results;

/**
 * A {@link SparqlClient} that answers queries with predefined {@link Results}s.
 */
public class ResultsSparqlClient extends AbstractSparqlClient {
    private final Map<SparqlQuery, Results> qry2results = new HashMap<>();
    private final List<Throwable> errors = new ArrayList<>();

    public ResultsSparqlClient(boolean nativeBind) {
        this(nativeBind, "http://example.org/sparql");
    }

    public ResultsSparqlClient(boolean nativeBind, String endpoint) {
        this(nativeBind, SparqlEndpoint.parse(endpoint));
    }

    public ResultsSparqlClient(boolean nativeBind, SparqlEndpoint endpoint) {
        super(endpoint);
        this.bindingAwareProtocol = nativeBind;
    }

    public @This ResultsSparqlClient answerWith(SparqlQuery query, Results results) {
        qry2results.put(query, results);
        return this;
    }

    public class BoundAnswersStage1 {
        private final SparqlQuery unboundQuery;
        private final Vars bindingsVars;
        private final Vars unboundVars;

        public BoundAnswersStage1(SparqlQuery unboundQuery, Vars bindingsVars, Vars unboundVars) {
            this.unboundQuery = unboundQuery;
            this.bindingsVars = bindingsVars;
            this.unboundVars = unboundVars;
        }

        public @This BoundAnswersStage2 answer(CharSequence... binding) {
            if (binding.length != bindingsVars.size())
                throw new IllegalArgumentException("Expected "+bindingsVars.size()+" terms");
            var rows = results(bindingsVars, (Object[]) binding).expected();
            if (rows.size() != 1)
                throw new IllegalArgumentException("Expected 1 binding, got "+rows.size());
            var bound = unboundQuery.bound(new RowBinding<>(LIST, bindingsVars).row(rows.get(0)));
            return new BoundAnswersStage2(this, bound);
        }

        public ResultsSparqlClient end() { return ResultsSparqlClient.this; }
    }

    public class BoundAnswersStage2 {
        private final BoundAnswersStage1 stage1;
        private final SparqlQuery boundQuery;

        public BoundAnswersStage2(BoundAnswersStage1 stage1, SparqlQuery boundQuery) {
            this.stage1 = stage1;
            this.boundQuery = boundQuery;
        }

        public BoundAnswersStage1 with(CharSequence... terms) {
            if (terms.length%stage1.unboundVars.size() != 0)
                throw new IllegalArgumentException("#terms not divisible by #unbound vars");
            //noinspection resource
            answerWith(boundQuery, results(stage1.unboundVars, (Object[]) terms));
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

    @Override public void close() { }

    @Override
    public <R> BIt<R> query(RowType<R> rowType, SparqlQuery sparql, @Nullable BIt<R> bindings,
                            @Nullable BindType type, @Nullable JoinMetrics metrics) {
        Results expected = qry2results.get(sparql);
        if (expected == null)
            throw new AssertionError("Unexpected query "+sparql);
        Results exBindings = expected.hasBindings() ? expected.bindingsAsResults()
                                                    : negativeResult();
        if (bindings == null) {
            if (!exBindings.isEmpty())
                throw error("Expected bindings, got null");
            else if (type != null)
                throw new IllegalArgumentException("null bindings with type == "+type);
            return rowType.convert(expected.asBIt());
        } else if (!expected.hasBindings()) {
            throw error("Did not expected bindings for "+sparql);
        } else if (type == null) {
            throw new NullPointerException("bindings != null but type == null");
        }
        if (usesBindingAwareProtocol()) {
            var cb = new CallbackBIt<>(rowType, expected.vars());
            Thread.startVirtualThread(() -> {
                try {
                    exBindings.check(bindings);
                    rowType.convert(expected.asBIt()).forEachRemaining(cb::feed);
                    cb.complete(null);
                } catch (Throwable t) {
                    cb.complete(t);
                }
            });
            return cb;
        } else {
            Vars unboundVars = expected.vars().minus(bindings.vars());
            for (List<Term> bindingRow : exBindings.expected()) {
                var bound = sparql.bound(new RowBinding<>(LIST, exBindings.vars()).row(bindingRow));
                Results boundExpected = qry2results.get(bound);
                if (boundExpected == null)
                    throw error("No result defined for bound query "+bound);
                if (boundExpected.hasBindings())
                    throw error("Bound query results expect bindings: "+bound);
                if (!boundExpected.vars().equals(unboundVars))
                    throw error("Bound query results vars do not match ");
            }
            return new ClientBindingBIt<>(bindings, type, this, sparql, metrics);
        }
    }

    private AssertionError error(String msg) {
        var e = new AssertionError(msg);
        errors.add(e);
        return e;
    }
}
