package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.util.owned.Orphan;

public non-sealed class EmitBindQuery<B extends Batch<B>> extends BindQuery<B> {
    public final Orphan<? extends Emitter<B, ?>> bindings;

    public EmitBindQuery(SparqlQuery query, Orphan<? extends Emitter<B, ?>> bindings,
                         BindType type) {
        super(query, type, null);
        if (bindings == null)
            throw new IllegalArgumentException("bindings cannot be null");
        this.bindings = bindings;
    }

    @Override public    Vars         bindingsVars() { return Emitter.peekVars(bindings); }
    @Override public    BatchType<B> batchType()    { return Emitter.peekBatchType(bindings); }
    @Override protected Object       bindingsObj()  { return bindings; }
}

