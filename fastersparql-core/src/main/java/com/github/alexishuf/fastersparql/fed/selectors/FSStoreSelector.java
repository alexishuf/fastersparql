package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException.SelectorTypeMismatch;
import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.store.StoreSparqlClient;

import java.io.IOException;

public class FSStoreSelector extends Selector {
    public static final String NAME = "store";
    private final StoreSparqlClient.Guard client;

    public FSStoreSelector(StoreSparqlClient client, Spec spec) {
        super(client.endpoint(), spec);
        this.client = client.retain();
        notifyInit(InitOrigin.LOAD, null);
    }

    public static class FSStoreLoader implements Loader {
        @Override
        public Selector load(SparqlClient client, Spec spec) throws IOException, BadSerializationException {
            if (!(client instanceof StoreSparqlClient ssc))
                throw new SelectorTypeMismatch("StoreSparqlClient", client.getClass().toString());
            return new FSStoreSelector(ssc, spec);
        }

        @Override public String name() { return NAME; }
    }

    @Override public void close() {
        super.close();
        client.close();
    }

    @Override public void saveIfEnabled() { /* no-op since we access the store directly */ }

    @Override public boolean has(TriplePattern tp) { return client.get().estimate(tp) > 0; }
}
