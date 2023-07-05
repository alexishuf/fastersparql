package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSIllegalStateException;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQueryType;
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
    private boolean closeCalled;
    protected boolean closed;
    @SuppressWarnings("unused") private int plainRefs;

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

    protected final void acquireRef() {
        int old = (int)REFS.getAndAddAcquire(this, 1);
        if (old <= 0) {
            REFS.getAndAddRelease(this, -1);
            throw mkClosed();
        }
    }

    protected final void releaseRef() {
        int old = (int) REFS.getAndAddRelease(this, -1);
        if (old == 1) {
            closed = true;
            doClose();
        } else if (old <= 0) {
            REFS.getAndAddAcquire(this, 1);
            log.error("Ignoring release without matching acquire",
                      new IllegalStateException(this + ": release without matching acquire"));
        }
    }

    protected abstract void doClose();

    @Override public final void close() {
        if (closeCalled) return;
        closeCalled = true;
        releaseRef();
    }

    protected FSIllegalStateException mkClosed() {
        return new FSIllegalStateException(endpoint, this+": already closed");
    }

    /* --- --- --- interface implementations --- --- --- */

    @Override public SparqlEndpoint endpoint() { return endpoint; }

    @Override public <B extends Batch<B>> BIt<B> query(BindQuery<B> q) {
        if (q.query.isGraph())
            throw new InvalidSparqlQueryType("query() method only takes SELECT/ASK queries");
        if (closed)
            throw mkClosed();
        try {
            return new ClientBindingBIt<>(q, this);
        } catch (Throwable t) {
            throw FSException.wrap(endpoint, t);
        }
    }

    @Override public final boolean usesBindingAwareProtocol() {
        return bindingAwareProtocol;
    }

    @Override public String toString() {
        String name = getClass().getSimpleName();
        if (name.endsWith("SparqlClient"))
            name = name.substring(0, name.length()-12);
        return name+'['+endpoint+']';
    }
}
