package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.BindingBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class ClientBindingBIt<B extends Batch<B>> extends BindingBIt<B> {
    private final SparqlClient client;
    private final SparqlQuery sparql;

    public ClientBindingBIt(BIt<B> left, BindType bindType, SparqlClient client,
                            SparqlQuery sparql, @Nullable JoinMetrics metrics) {
        super(left, bindType, sparql.publicVars(), null, metrics);
        this.client        = client;
        this.sparql        = sparql;
    }

    @Override protected BIt<B> bind(BatchBinding<B> binding) {
        return client.query(batchType, sparql.bound(binding));
    }

    @Override protected Object rightUnbound() {
        return new ByteRope().append("Query[").append(client.endpoint().uri())
                .append(']').append('(').escapingLF(sparql.sparql()).append(')')
                .toString();
    }
}
