package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.store.StoreSparqlClient;

public class FSStoreSelector extends Selector {
    public static final String NAME = "store";
    private final StoreSparqlClient.Ref client;

    public FSStoreSelector(StoreSparqlClient client, Spec spec) {
        super(client.endpoint(), spec);
        this.client = client.liveRef();
    }

    @Override public void close() {
        super.close();
        client.close();
    }

    @Override public void saveIfEnabled() { /* no-op since we access the store directly */ }

    @Override public boolean has(TriplePattern tp) {
        return client.get().estimate(tp) > 0;
    }
}
