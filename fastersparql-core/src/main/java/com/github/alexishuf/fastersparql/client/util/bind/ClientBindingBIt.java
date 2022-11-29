package com.github.alexishuf.fastersparql.client.util.bind;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;

public final class ClientBindingBIt<R, I> extends BindingBIt<R, I> {
    private final SparqlClient<R, I, ?> client;
    private final SparqlQuery sparql;

    public ClientBindingBIt(BIt<R> left, BindType bindType, RowType<R, I> rowType,
                            SparqlClient<R, I, ?> client,
                            SparqlQuery sparql) {
        super(left, bindType, rowType, left.vars(), sparql.publicVars(), null);
        this.client        = client;
        this.sparql        = sparql;
    }

    @Override protected BIt<R> bind(R input) {
        return client.query(sparql.bind(tempBinding.row(input)));
    }
}
