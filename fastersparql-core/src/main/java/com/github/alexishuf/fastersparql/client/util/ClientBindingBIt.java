package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.util.BindingBIt;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class ClientBindingBIt<R> extends BindingBIt<R> {
    private final SparqlClient client;
    private final SparqlQuery sparql;

    public ClientBindingBIt(BIt<R> left, BindType bindType, SparqlClient client,
                            SparqlQuery sparql, @Nullable JoinMetrics metrics) {
        super(left, bindType, left.vars(), sparql.publicVars(), null, metrics);
        this.client        = client;
        this.sparql        = sparql;
    }

    @Override protected BIt<R> bind(R input) {
        return client.query(rowType, sparql.bound(tempBinding.row(input)));
    }

    @Override protected Object right() {
        return new ByteRope().append("Query[").append(client.endpoint().uri())
                .append(']').append('(').escapingLF(sparql.sparql()).append(')')
                .toString();
    }
}
