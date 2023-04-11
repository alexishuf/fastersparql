package com.github.alexishuf.fastersparql.hdt;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.base.UnitaryBIt;
import com.github.alexishuf.fastersparql.batch.operators.BindingBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQueryType;
import com.github.alexishuf.fastersparql.fed.Federation;
import com.github.alexishuf.fastersparql.fed.Source;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.fed.selectors.TrivialSelector;
import com.github.alexishuf.fastersparql.hdt.batch.HdtBatch;
import com.github.alexishuf.fastersparql.hdt.batch.IdAccess;
import com.github.alexishuf.fastersparql.hdt.cardinality.HdtCardinalityEstimator;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.VarHandle;
import java.util.concurrent.CompletionStage;

import static com.github.alexishuf.fastersparql.hdt.FSHdtProperties.estimatorPeek;
import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.*;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static org.rdfhdt.hdt.enums.TripleComponentRole.*;

public class HdtSparqlClient extends AbstractSparqlClient {
    private static final Logger log = LoggerFactory.getLogger(HdtSparqlClient.class);
    private static final VarHandle HDT_REFS;
    static {
        try {
            HDT_REFS = lookup().findVarHandle(HdtSparqlClient.class, "plainHdtRefs", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final HDT hdt;
    private final Federation federation;
    final int dictId;
    private final CompletionStage<HdtSparqlClient> estimatorReady;
    @SuppressWarnings("unused") // accessed through DICT_REFS
    private int plainHdtRefs;

    public HdtSparqlClient(SparqlEndpoint ep) {
        super(ep);
        this.bindingAwareProtocol = true;
        if (ep.protocol() != Protocol.FILE)
            throw new FSInvalidArgument("HdtSparqlClient requires file:// endpoint");
        var path = endpoint.asFile().getAbsolutePath();
        var listener = new LogProgressListener(path);
        try {
            this.hdt = HDTManager.mapIndexedHDT(path, listener);
        } catch (Throwable t) {
            listener.complete(t);
            throw FSException.wrap(ep, t);
        }
        dictId = IdAccess.register(hdt.getDictionary());
        HDT_REFS.setRelease(this, 1);
        federation = new Federation(ep, null);
        var estimator = new HdtCardinalityEstimator(hdt, dictId, estimatorPeek(), ep.toString());
        estimatorReady = estimator.ready().thenApply(ignored -> this);
        var selector = new TrivialSelector(ep, Spec.EMPTY);
        federation.addSource(new Source(this, selector, estimator, Spec.EMPTY));
    }

    public CompletionStage<HdtSparqlClient> estimatorReady() {
        return estimatorReady;
    }

    @Override public <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql) {
        BIt<HdtBatch> hdtIt;
        if (sparql instanceof TriplePattern tp) {
            long s, p, o;
            Vars vars = sparql.publicVars();
            var dict = hdt.getDictionary();
            if (       (s = plain(dict, tp.s,   SUBJECT)) == NOT_FOUND
                    || (p = plain(dict, tp.p, PREDICATE)) == NOT_FOUND
                    || (o = plain(dict, tp.o,    OBJECT)) == NOT_FOUND) {
                hdtIt = new EmptyBIt<>(HdtBatch.TYPE, vars);
            } else {
                var it = hdt.getTriples().search(new TripleID(s, p, o));
                hdtIt = new HdtIteratorBIt(tp, it);
            }
        } else {
            hdtIt = federation.query(HdtBatch.TYPE, sparql);
        }
        return batchType.convert(hdtIt);
    }

    @Override
    public <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql, @Nullable BIt<B> bindings, @Nullable BindType type, Metrics.@Nullable JoinMetrics metrics) {
        if (bindings == null || bindings instanceof EmptyBIt<B>)
            return query(batchType, sparql);
        else if (type == null)
            throw new NullPointerException("bindings != null, but type is null!");
        if (sparql.isGraph())
            throw new InvalidSparqlQueryType("query() method only takes SELECT/ASK queries");
        try {
            Plan query = new SparqlParser().parse(sparql);
            if (batchType == HdtBatch.TYPE && query instanceof TriplePattern tp) //noinspection unchecked
                return (BIt<B>) new HdtBindingBIt((BIt<HdtBatch>) bindings, type, tp, metrics);
            return new ClientBindingBIt<>(bindings, type, this, query, metrics);
        } catch (Throwable t) {
            throw FSException.wrap(endpoint, t);
        }
    }

    private void acquireHdt() {
        if  ((int) HDT_REFS.getAndAdd(this, 1) == 0) {
            HDT_REFS.getAndAdd(this, -1);
            throw new IllegalStateException(this+" closed, cannot access HDT");
        }
    }
    private void releaseHdt() {
        if ((int) HDT_REFS.getAndAdd(this, -1) == 1) {
            IdAccess.release(dictId);
            log.info("Closing {}", this);
            try {
                hdt.close();
            } catch (Throwable t) {
                log.error("Ignoring failure to close HDT object for {}", endpoint, t);
            }
        }
    }

    @Override public void close() {
        releaseHdt();
    }

    /* --- --- --- inner classes --- --- --- */
    final class HdtIteratorBIt extends UnitaryBIt<HdtBatch> {

        private final IteratorTripleID it;
        private final byte v0Role;
        private final byte v1Role;
        private final byte v2Role;

        public HdtIteratorBIt(TriplePattern tp, IteratorTripleID it) {
            this(tp.publicVars(), tp.s, tp.p, tp.o, it);
        }
        public HdtIteratorBIt(Vars vars, Term s, Term p, Term o, IteratorTripleID it) {
            super(HdtBatch.TYPE, vars);
            acquireHdt();
            this.it = it;

            final int n = vars.size();
            // set v0Role
            if      (n               == 0) v0Role = 0;
            else if (vars.indexOf(s) == 0) v0Role = 1;
            else if (vars.indexOf(p) == 0) v0Role = 2;
            else if (vars.indexOf(o) == 0) v0Role = 3;
            else                           v0Role = 0;
            // set v1Role
            if      (n               <  2) v1Role = 0;
            else if (vars.indexOf(p) == 1) v1Role = 2;
            else if (vars.indexOf(o) == 1) v1Role = 3;
            else                           v1Role = 0;
            // set v2Role
            if      (n > 2 && vars.indexOf(o) == 2) v2Role = 3;
            else                                    v2Role = 0;
        }

        private static void putRole(int col, HdtBatch dest, byte role, TripleID triple, int dictId) {
            switch (role) {
                case 0 -> {}
                case 1 -> dest.putTerm(col, encode(triple.getSubject(),   dictId,   SUBJECT));
                case 2 -> dest.putTerm(col, encode(triple.getPredicate(), dictId, PREDICATE));
                case 3 -> dest.putTerm(col, encode(triple.getObject(),    dictId,    OBJECT));
                default -> throw new IllegalArgumentException();
            }
        }

        @Override protected boolean fetch(HdtBatch dest) {
            if (!it.hasNext()) return false;
            TripleID t = it.next();
            dest.beginPut();
            int dictId = HdtSparqlClient.this.dictId;
            putRole(0, dest, v0Role, t, dictId);
            putRole(1, dest, v1Role, t, dictId);
            putRole(2, dest, v2Role, t, dictId);
            dest.commitPut();
            return true;
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            super.cleanup(cause);
            releaseHdt();
        }
    }

    public final class HdtBindingBIt extends BindingBIt<HdtBatch> {
        private final TriplePattern right;
        private final long s, p, o;
        private final @Nullable BIt<HdtBatch> empty;
        private final Vars rightFreeVars;
        private final Dictionary dict;

        public HdtBindingBIt(BIt<HdtBatch> left, BindType type, TriplePattern right,
                             Metrics.@Nullable JoinMetrics metrics) {
            super(left, type, right.publicVars(), null, metrics);
            acquireHdt();
            this.right = right;
            this.dict = hdt.getDictionary();
            this.s = plain(dict, right.s,   SUBJECT);
            this.p = plain(dict, right.p, PREDICATE);
            this.o = plain(dict, right.o,    OBJECT);
            this.rightFreeVars = right.publicVars().minus(left.vars());
            if (s == -1 || p == -1 || o == -1)
                empty = new EmptyBIt<>(HdtBatch.TYPE, rightFreeVars);
            else
                empty = null;
        }

        @Override protected Object rightUnbound() {
            return right;
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            super.cleanup(cause);
            releaseHdt();
        }

        private long bind(Term term, long sourcedId, TripleComponentRole role,
                          long[] batch, int base, Vars vars) {
            int col = vars.indexOf(term);
            if (col == -1) return sourcedId;
            return plainIn(dict, role,  batch[base+col]);
        }

        @Override protected BIt<HdtBatch> bind(BatchBinding<HdtBatch> binding) {
            if (empty != null) return empty;
            TripleID query;
            HdtBatch bindingBatch = binding.batch;
            if (bindingBatch == null) {
                query = new TripleID(s, p, o);
            } else {
                long[] batch = bindingBatch.arr();
                int base = binding.row*bindingBatch.cols;
                Vars vars = binding.vars;
                query = new TripleID(bind(right.s, s, SUBJECT,   batch, base, vars),
                                     bind(right.p, p, PREDICATE, batch, base, vars),
                                     bind(right.o, o, OBJECT,    batch, base, vars));
            }
            Term s = binding.getIf(right.s);
            Term p = binding.getIf(right.p);
            Term o = binding.getIf(right.o);
            return new HdtIteratorBIt(rightFreeVars, s, p, o, hdt.getTriples().search(query));
        }
    }

    private static final class LogProgressListener implements ProgressListener {
        private final String path;
        private final long interval = FSHdtProperties.mapProgressIntervalMs()*1_000_000L;
        private long lastMessage = System.nanoTime();
        private boolean logged;

        public LogProgressListener(String path) {
            this.path = path;
        }

        @Override public void notifyProgress(float level, String message) {
            long now = System.nanoTime();
            long elapsed = now - lastMessage;
            if (elapsed > interval) {
                lastMessage = now;
                log.info("mapping/indexing {}: {}%...", path, format("%.2f", level));
                logged = true;
            }
        }

        public void complete(@Nullable Throwable error) {
            if (error != null)
                log.error("failed to map/index {}", path, error);
            else if (logged)
                log.info("Mapped/indexed {}", path);
        }
    }
}
