package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.BindingBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.ItBindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;

public final class ClientBindingBIt<B extends Batch<B>> extends BindingBIt<B> {
    private final SparqlClient client;

    public ClientBindingBIt(ItBindQuery<B> bindQuery, SparqlClient client) {
        super(bindQuery, null);
        this.client = client;
        addGuard(client.retain());
    }

    @Override protected BIt<B> bind(BatchBinding<B> binding) {
        return client.query(batchType, bindQuery.query.bound(binding));
    }
}
