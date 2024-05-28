package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.FailedBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.EmitBindQuery;
import com.github.alexishuf.fastersparql.client.ItBindQuery;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.netty.NettySparqlClient;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Emitters;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.util.owned.Orphan;

public class ProcessNettySparqlClient extends NettySparqlClient {
    private final ServerProcess process;

    public ProcessNettySparqlClient(SparqlEndpoint ep, ServerProcess serverProcess) {
        super(ep);
        this.process = serverProcess;
    }

    @Override protected void doClose() {
        try {
            super.doClose();
        } finally {
            process.close();
        }
    }

    @Override public String toString() {
        return super.toString()+"@"+process.name();
    }

    @Override
    protected <B extends Batch<B>> Orphan<? extends Emitter<B, ?>> doEmit(BatchType<B> bt, SparqlQuery sparql, Vars rebindHint) {
        if (process.isDead())
            return Emitters.error(bt, sparql.publicVars(), process.makeDeadException(endpoint));
        return super.doEmit(bt, sparql, rebindHint);
    }

    @Override
    protected <B extends Batch<B>> Orphan<? extends Emitter<B, ?>> doEmit(EmitBindQuery<B> query, Vars rebindHint) {
        if (process.isDead())
            return Emitters.error(query.batchType(), query.resultVars(), process.makeDeadException(endpoint));
        return super.doEmit(query, rebindHint);
    }

    @Override protected <B extends Batch<B>> BIt<B> doQuery(BatchType<B> bt, SparqlQuery sp) {
        if (process.isDead())
            return new FailedBIt<>(bt, sp.publicVars(), process.makeDeadException(endpoint));
        return super.doQuery(bt, sp);
    }

    @Override protected <B extends Batch<B>> BIt<B> doQuery(ItBindQuery<B> bq) {
        if (process.isDead())
            return new FailedBIt<>(bq.batchType(), bq.resultVars(), process.makeDeadException(endpoint));
        return super.doQuery(bq);
    }
}
