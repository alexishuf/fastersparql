package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.stages.BindingStage;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSIllegalStateException;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQueryType;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public abstract class AbstractSparqlClient implements SparqlClient {
    private static final Logger log = LoggerFactory.getLogger(AbstractSparqlClient.class);
    private static final VarHandle REFS;
    static {
        try {
            REFS = MethodHandles.lookup().findVarHandle(AbstractSparqlClient.class, "plainRefs", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    protected final SparqlEndpoint endpoint;
    protected boolean bindingAwareProtocol;
    protected boolean localInProcess;
    private boolean closeCalled;
    protected DistinctType cheapestDistinct = DistinctType.REDUCED;
    @SuppressWarnings("unused") private int plainRefs;
    private @Nullable Exception closedAt;

    /* --- --- --- lifecycle --- --- --- */

    public AbstractSparqlClient(SparqlEndpoint ep) {
        this.endpoint = ep;
        REFS.setRelease(this, 1);
    }

    protected static final class NoOpGuard implements Guard {
        public static final NoOpGuard INSTANCE = new NoOpGuard();
        @Override public void close() {}
    }

    protected class RefGuard implements Guard {
        boolean closed;
        public RefGuard() { acquireRef(); }
        @Override public void close() {
            if (closed) return;
            closed = true;
            releaseRef();
        }
    }

    protected final void acquireRef() throws FSIllegalStateException {
        if ((int)REFS.getAndAddAcquire(this, 1) <= 0)
            badAcquireRef();
    }

    private void badAcquireRef() {
        REFS.getAndAddRelease(this, -1);
        throw new FSIllegalStateException(endpoint, this + ": already closed", closedAt);
    }

    protected final void releaseRef() {
        int old = (int) REFS.getAndAddRelease(this, -1);
        if (old == 1) {
            if (ThreadJournal.ENABLED)
                ThreadJournal.journal("closing client=", this);
            if (AbstractSparqlClient.class.desiredAssertionStatus())
                closedAt = new Exception("closed here");
            log.debug("Closing {}", this);
            doClose();
        } else if (old <= 0) {
            badReleaseRef();
        }
    }

    private void badReleaseRef() {
        REFS.getAndAddAcquire(this, 1);
        log.error("Ignoring release without matching acquire",
                  new IllegalStateException(this + ": release without matching acquire"));
    }

    protected abstract void doClose();

    @Override public final void close() {
        if (closeCalled) return;
        closeCalled = true;
        releaseRef();
    }

    /* --- --- --- query implementations --- --- --- */

    protected abstract <B extends Batch<B>> BIt<B> doQuery(BatchType<B> bt, SparqlQuery sparql);
    protected abstract <B extends Batch<B>> Emitter<B> doEmit(BatchType<B> bt, SparqlQuery sparql,
                                                              Vars rebindHint);

    protected <B extends Batch<B>> BIt<B> doQuery(ItBindQuery<B> bq) {
        return new ClientBindingBIt<>(bq, this);
    }

    protected <B extends Batch<B>> Emitter<B> doEmit(EmitBindQuery<B> query, Vars rebindHint) {
        return new BindingStage<>(query, rebindHint, this);
    }

    /* --- --- --- interface implementations --- --- --- */

    @Override public SparqlEndpoint endpoint() { return endpoint; }

    @Override public final <B extends Batch<B>> BIt<B> query(BatchType<B> bt, SparqlQuery sparql) {
        if (sparql.isGraph())
            throw new InvalidSparqlQueryType("query() method only takes SELECT/ASK queries");
        acquireRef();
        try {
            return doQuery(bt, sparql);
        } catch (Throwable t) {
            throw FSException.wrap(endpoint, t);
        } finally {
            releaseRef();
        }
    }

    @Override
    public final <B extends Batch<B>> Emitter<B> emit(BatchType<B> bt, SparqlQuery sparql,
                                                      Vars rebindHint) {
        if (sparql.isGraph())
            throw new InvalidSparqlQueryType("query() method only takes SELECT/ASK queries");
        acquireRef();
        try {
            return doEmit(bt, sparql, rebindHint);
        } catch (Throwable t) {
            throw FSException.wrap(endpoint, t);
        } finally {
            releaseRef();
        }
    }

    @Override public final <B extends Batch<B>> BIt<B> query(ItBindQuery<B> q) {
        if (q.query.isGraph())
            throw new InvalidSparqlQueryType("query() method only takes SELECT/ASK queries");
        acquireRef();
        try {
            BIt<B> bindings = q.bindings;
            if (q.type == BindType.MINUS && !bindings.vars().intersects(q.nonAskQuery.publicVars()))
                return bindings;
            return doQuery(q);
        } catch (Throwable t) {
            throw FSException.wrap(endpoint, t);
        } finally {
            releaseRef();
        }
    }

    @Override public final <B extends Batch<B>> Emitter<B> emit(EmitBindQuery<B> q,
                                                                Vars rebindHint) {
        if (q.query.isGraph())
            throw new InvalidSparqlQueryType("emit() method only takes SELECT/ASK queries");
        acquireRef();
        try {
            var bindings = q.bindings;
            if (q.type == BindType.MINUS && !bindings.vars().intersects(q.nonAskQuery.publicVars()))
                return bindings;
            return doEmit(q, rebindHint);
        } catch (Throwable t) {
            throw FSException.wrap(endpoint, t);
        } finally {
            releaseRef();
        }
    }

    @Override public final boolean usesBindingAwareProtocol() { return bindingAwareProtocol; }
    @Override public DistinctType          cheapestDistinct() { return cheapestDistinct; }
    @Override public boolean               isLocalInProcess() { return localInProcess; }

    @Override public String toString() {
        String name = getClass().getSimpleName();
        if (name.endsWith("SparqlClient"))
            name = name.substring(0, name.length()-12);
        return name+'['+endpoint+']';
    }
}
