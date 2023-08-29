package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;

public non-sealed class EmitBindQuery<B extends Batch<B>> extends BindQuery<B> {
    public final Emitter<B> bindings;

    public EmitBindQuery(SparqlQuery query, Emitter<B> bindings, BindType type) {
        super(query, type, null);
        if (bindings == null)
            throw new IllegalArgumentException("bindings cannot be null");
        this.bindings = bindings;
    }

    @Override public    Vars         bindingsVars() { return bindings.vars(); }
    @Override public    BatchType<B> batchType()    { return bindings.batchType(); }
    @Override protected Object       bindingsObj()  { return bindings; }
}

