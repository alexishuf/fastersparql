package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.org.apache.jena.query.Dataset;
import com.github.alexishuf.fastersparql.org.apache.jena.query.Query;
import com.github.alexishuf.fastersparql.org.apache.jena.query.QueryFactory;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.core.DatasetGraph;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.core.Transactional;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.exec.QueryExec;
import com.github.alexishuf.fastersparql.org.apache.jena.sparql.exec.QueryExecBuilder;
import com.github.alexishuf.fastersparql.org.apache.jena.tdb2.TDB2Factory;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;

public class Tdb2SparqlClient extends AbstractSparqlClient {
    private static final Logger log = LoggerFactory.getLogger(Tdb2SparqlClient.class);
    private final Dataset dataset;
    private final DatasetGraph dsg;

    public Tdb2SparqlClient(SparqlEndpoint endpoint) {
        super(endpoint);
        File file = endpoint.asFile();
        if (!file.exists())
            throw new FSException(endpoint + " does not exist");
        if (file.isFile())
            dataset = TDB2Factory.assembleDataset(file.getAbsolutePath());
        else
            dataset = TDB2Factory.connectDataset(file.getAbsolutePath());
        dsg = Objects.requireNonNull(dataset.asDatasetGraph());
    }

    @Override public Guard retain() {return new RefGuard();}

    @Override protected void doClose() {
        dataset.close();
    }

    @Override protected <B extends Batch<B>> BIt<B> doQuery(BatchType<B> bt, SparqlQuery sparql) {
        QueryExec exec = null;
        try {
            var query = QueryFactory.create(sparql.sparql().toString());
            exec = QueryExec.dataset(dsg).query(query).build();
            var it = new RefJenaBIt<>(bt, sparql.publicVars(), dsg, exec, sparql.isAsk());
            exec = null;
            return it;
        } finally {
            if (exec != null) {
                try {
                    exec.close();
                } catch (Throwable t) { log.error("Failed to close QueryExecution", t); }
            }
        }
    }

    private final class RefJenaBIt<B extends Batch<B>> extends JenaBIt<B> {
        public RefJenaBIt(BatchType<B> batchType, Vars vars, Transactional transactional, QueryExec exec, boolean ask) {
            super(batchType, vars, transactional, exec, ask);
            acquireRef();
        }
        @Override protected void cleanup(@Nullable Throwable cause) {
            try {
                super.cleanup(cause);
            } finally { releaseRef(); }
        }
    }

    private static abstract class RefJenaEmitter<B extends Batch<B>>
            extends JenaEmitter<B, RefJenaEmitter<B>> {
        private final Tdb2SparqlClient client;

        private RefJenaEmitter(Tdb2SparqlClient client, BatchType<B> batchType, Vars vars,
                               Transactional transactional,
                               Query query, QueryExecBuilder execFac) {
            super(batchType, vars, transactional, query, execFac);
            this.client = client;
            client.acquireRef();
        }

        private static final class Concrete<B extends Batch<B>>
                extends RefJenaEmitter<B> implements Orphan<RefJenaEmitter<B>> {
            private Concrete(Tdb2SparqlClient client, BatchType<B> batchType, Vars vars,
                               Transactional transactional,
                               Query query, QueryExecBuilder execFac) {
                super(client, batchType, vars, transactional, query, execFac);
            }
            @Override public RefJenaEmitter<B> takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override protected void doRelease() {
            try {
                super.doRelease();
            } finally { client.releaseRef(); }
        }
    }

    @Override
    protected <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    doEmit(BatchType<B> bt, SparqlQuery sparql, Vars rebindHint) {
        var query = QueryFactory.create(sparql.sparql().toString());
        var execFac = QueryExec.dataset(dsg);
        return new RefJenaEmitter.Concrete<>(this, bt, sparql.publicVars(),
                                             dsg, query, execFac);
    }
}
