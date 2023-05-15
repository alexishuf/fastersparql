package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;

import java.io.IOException;

public final class TrivialSelector extends Selector {
    public static final String NAME = "trivial";
    public static final String RESULT = "result";
    private final boolean result;

    public static final class TrivialLoader implements Loader {
        @Override public String name() { return NAME; }

        @Override
        public Selector load(SparqlClient client, Spec spec) throws IOException {
            return new TrivialSelector(client.endpoint(), spec);
        }
    }

    public TrivialSelector(SparqlEndpoint endpoint, Spec spec) {
        super(endpoint, spec);
        result = spec.getOr(RESULT, true);
        notifyInit(InitOrigin.SPEC, null);
    }

    @Override public void saveIfEnabled() {
        // do nothing as we have no state to save
    }

    @Override public boolean has(TriplePattern tp) { return result; }
}
