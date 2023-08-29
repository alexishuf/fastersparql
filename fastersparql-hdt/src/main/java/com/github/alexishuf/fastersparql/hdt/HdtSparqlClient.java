package com.github.alexishuf.fastersparql.hdt;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.base.UnitaryBIt;
import com.github.alexishuf.fastersparql.batch.operators.BindingBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.EmitBindQuery;
import com.github.alexishuf.fastersparql.client.ItBindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.async.EmitterService;
import com.github.alexishuf.fastersparql.emit.async.SelfEmitter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.stages.BindingStage;
import com.github.alexishuf.fastersparql.emit.stages.ConverterStage;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;
import com.github.alexishuf.fastersparql.fed.CardinalityEstimatorProvider;
import com.github.alexishuf.fastersparql.fed.SingletonFederator;
import com.github.alexishuf.fastersparql.hdt.batch.HdtBatch;
import com.github.alexishuf.fastersparql.hdt.batch.IdAccess;
import com.github.alexishuf.fastersparql.hdt.cardinality.HdtCardinalityEstimator;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.Modifier;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.Triples;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.batch.type.Batch.*;
import static com.github.alexishuf.fastersparql.hdt.FSHdtProperties.estimatorPeek;
import static com.github.alexishuf.fastersparql.hdt.batch.HdtBatch.TYPE;
import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.*;
import static java.lang.String.format;
import static org.rdfhdt.hdt.enums.TripleComponentRole.*;

public class HdtSparqlClient extends AbstractSparqlClient implements CardinalityEstimatorProvider {
    private static final Logger log = LoggerFactory.getLogger(HdtSparqlClient.class);

    private final HDT hdt;
    final int dictId;
    private final HdtSingletonFederator federator;
    private final HdtCardinalityEstimator estimator;

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
        estimator = new HdtCardinalityEstimator(hdt, estimatorPeek(), ep.toString());
        federator = new HdtSingletonFederator(this, estimator);
    }

    @Override public SparqlClient.Guard retain() { return new RefGuard(); }

    @Override protected void doClose() {
        log.debug("Closing {}", this);
        try {
            Async.waitStage(estimator.ready());
        } catch (Throwable t) {
            log.info("Ignoring estimator failure", t);
        }
        IdAccess.release(dictId);
        try {
            hdt.close();
        } catch (Throwable t) {
            log.error("Ignoring failure to close HDT object for {}", endpoint, t);
        }
    }

    private final class HdtSingletonFederator extends SingletonFederator {
        public HdtSingletonFederator(HdtSparqlClient client, HdtCardinalityEstimator estimator) {
            super(client, HdtBatch.TYPE, estimator);
        }

        @Override
        protected <B extends Batch<B>> Emitter<B> convert(BatchType<B> dest, Emitter<?> in) {
            //noinspection unchecked
            return new FromHdtConverter<>(dest, (Emitter<HdtBatch>) in);
        }
    }

    @Override public HdtCardinalityEstimator estimator() { return estimator; }

    @Override protected <B extends Batch<B>> BIt<B> doQuery(BatchType<B> bt, SparqlQuery sparql) {
        var plan = SparqlParser.parse(sparql);
        BIt<HdtBatch> hdtIt;
        if (plan instanceof Modifier m && m.left instanceof TriplePattern tp) {
            Vars vars = m.filters.isEmpty() ? m.publicVars() : tp.publicVars();
            hdtIt = m.executeFor(queryTP(vars, tp), null, false);
        } else if (plan instanceof TriplePattern tp) {
            hdtIt = queryTP(tp.publicVars(), tp);
        } else {
            hdtIt = federator.execute(TYPE, plan);
        }
        return bt.convert(hdtIt);
    }

    @Override protected <B extends Batch<B>> Emitter<B>
    doEmit(BatchType<B> bt, SparqlQuery sparql) {
        var plan = SparqlParser.parse(sparql);
        Emitter<HdtBatch> hdtEm;
        Modifier m = plan instanceof Modifier mod ? mod : null;
        if ((m == null ? plan : m.left) instanceof TriplePattern tp) {
            Vars vars = (m != null && m.filters.isEmpty() ? m : tp).publicVars();
            hdtEm = new TPEmitter(tp, vars);
            if (m != null)
                hdtEm = m.processed(hdtEm);
        } else {
            hdtEm = federator.emit(TYPE, plan);
        }
        //noinspection unchecked
        return bt ==  TYPE ? (Emitter<B>) hdtEm : new FromHdtConverter<>(bt, hdtEm);
    }

    private BIt<HdtBatch> queryTP(Vars vars, TriplePattern tp) {
        long s, p, o;
        BIt<HdtBatch> it;
        var dict = hdt.getDictionary();
        if (       (s = plain(dict, tp.s,   SUBJECT)) == NOT_FOUND
                || (p = plain(dict, tp.p, PREDICATE)) == NOT_FOUND
                || (o = plain(dict, tp.o,    OBJECT)) == NOT_FOUND) {
            it = new EmptyBIt<>(TYPE, vars);
        } else {
            var hdtIt = hdt.getTriples().search(new TripleID(s, p, o));
            it = new HdtIteratorBIt(vars, tp.s, tp.p, tp.o, hdtIt);
        }
        var metrics = Metrics.createIf(tp);
        if (metrics != null)
            it.metrics(metrics);
        return it;
    }

    @Override protected <B extends Batch<B>> BIt<B> doQuery(ItBindQuery<B> bq) {
        Plan q = bq.parsedQuery();
        if (bq.bindings.batchType() == TYPE && (q instanceof TriplePattern
                || (q instanceof Modifier m && m.left instanceof TriplePattern))) {
            //noinspection unchecked
            return (BIt<B>) new HdtBindingBIt((ItBindQuery<HdtBatch>) bq, q);
        }
        return new ClientBindingBIt<>(bq, this);
    }

    @Override protected <B extends Batch<B>> Emitter<B> doEmit(EmitBindQuery<B> bq) {
        return new BindingStage.ForPlan<>(bq, false, null);
    }

    /* --- --- --- emitters --- --- --- */

    final class FromHdtConverter<B extends Batch<B>> extends ConverterStage<HdtBatch, B> {
        private final short dictId;

        public FromHdtConverter(BatchType<B> type, Emitter<HdtBatch> upstream) {
            super(type, upstream);
            this.dictId = (short)HdtSparqlClient.this.dictId;
        }

        @Override protected BatchBinding<HdtBatch> convertBinding(BatchBinding<B> binding) {
            B b = binding.batch;
            HdtBatch conv = b == null ? null
                    : TYPE.create(1, b.cols, 0)
                          .putRowConverting(b, binding.row, dictId);
            var bb = new BatchBinding<HdtBatch>(binding.vars);
            bb.setRow(conv, 0);
            return bb;
        }
    }

    final class TPEmitter extends SelfEmitter<HdtBatch> {
        private static final BatchBinding<HdtBatch> EMPTY_BINDING;
        static {
            EMPTY_BINDING = new BatchBinding<>(Vars.EMPTY);
            var b = TYPE.create(1, 0, 0);
            b.beginPut();
            b.commitPut();
            EMPTY_BINDING.setRow(b, 0);
        }

        private static final int LIMIT_TICKS = 4;
        private static final int INIT_ROWS = 64;
        private static final int MAX_BATCH = BIt.DEF_MAX_BATCH;
        private static final int TIME_CHK_MASK = 0x7;
        private static final int HAS_UNSET_OUT = 0x01000000;
        private static final Flags FLAGS = SELF_EMITTER_FLAGS.toBuilder()
                .flag(HAS_UNSET_OUT, "UNSET_OUT").build();

        private @Nullable HdtBatch recycled;
        private IteratorTripleID it;
        private final short dictId = (short)HdtSparqlClient.this.dictId;
        private final byte cols;
        private final byte sOutCol, pOutCol, oOutCol;
        private long[] rowSkel;
        // ---------- fields below this line are accessed only on construction/rebind()
        private @MonotonicNonNull Vars lastBindingVars;
        private byte sInCol, pInCol, oInCol;
        private final TriplePattern tp;
        private final Dictionary dict = HdtSparqlClient.this.hdt.getDictionary();
        private final Triples triples = HdtSparqlClient.this.hdt.getTriples();
        private Term view = Term.pooledMutable();
        private final TripleID search = new TripleID();

        public TPEmitter(TriplePattern tp, Vars outVars) {
            super(TYPE, outVars, EmitterService.EMITTER_SVC, RR_WORKER, CREATED, FLAGS);
            int cols = outVars.size();
            int sOutCol = outVars.indexOf(tp.s);
            int pOutCol = outVars.indexOf(tp.p);
            int oOutCol = outVars.indexOf(tp.o);
            if (cols > 0x7f || sOutCol > 0x7f || pOutCol > 0x7f || oOutCol > 0x7f)
                throw new IllegalArgumentException("Too many columns");
            this.sOutCol = (byte)sOutCol;
            this.pOutCol = (byte)pOutCol;
            this.oOutCol = (byte)oOutCol;
            this.cols = (byte)cols;
            this.tp = tp;
            Arrays.fill(rowSkel = ArrayPool.longsAtLeast(cols), 0L);
            if (!tp.publicVars().containsAll(outVars))
                setFlagsRelease(statePlain(), HAS_UNSET_OUT);
            search.setSubject  (plain(dict, tp.s, SUBJECT));
            search.setPredicate(plain(dict, tp.p, PREDICATE));
            search.setObject   (plain(dict, tp.o, OBJECT));
            rebind(BatchBinding.ofEmpty(TYPE));
            it = triples.search(search);
            acquireRef();
        }

        @Override protected void doRelease() {
            try {
                recycled = recyclePooled(recycled);
                ArrayPool.LONG.offer(rowSkel, rowSkel.length);
                rowSkel = ArrayPool.EMPTY_LONG;
                view.recycle();
                view = Term.EMPTY_STRING;
                releaseRef();
            } finally {
                super.doRelease();
            }
        }

        private int bindingsVarsChanged(int state, Vars bVars) {
            lastBindingVars = vars;
            int sInCol = bVars.indexOf(tp.s);
            int pInCol = bVars.indexOf(tp.p);
            int oInCol = bVars.indexOf(tp.o);
            if (sInCol > 0x7f || pInCol > 0x7f || oInCol > 0x7f)
                throw new IllegalArgumentException("Too many binding vars");
            this.sInCol = (byte)sInCol;
            this.pInCol = (byte)pInCol;
            this.oInCol = (byte)oInCol;
            var tpVars = tp.publicVars();
            for (SegmentRope outVar : vars) {
                if (!tpVars.contains(outVar) || bVars.contains(outVar))
                    return setFlagsRelease(state, HAS_UNSET_OUT);
            }
            return clearFlagsRelease(state, HAS_UNSET_OUT);
        }

        @Override public void rebind(BatchBinding<HdtBatch> binding) throws RebindException {
            Vars bVars = binding.vars;
            if (EmitterStats.ENABLED  && stats != null)
                stats.onRebind(binding);
            int st = resetForRebind(0, LOCKED_MASK);
            try {
                if (!bVars.equals(lastBindingVars))
                    st = bindingsVarsChanged(st, bVars);
                if ((st&HAS_UNSET_OUT) != 0) {
                    HdtBatch bBatch = binding.batch;
                    if (bBatch == null || binding.row >= bBatch.rows)
                        throw new IllegalArgumentException("Bad batch/row for binding");
                    int base = binding.row*bBatch.cols;
                    long[] ids = bBatch.arr;
                    for (int c = 0, bc; c < cols; c++)
                        rowSkel[c] = (bc = bVars.indexOf(vars.get(c))) < 0 ? NOT_FOUND : ids[base+bc];
                }
                search.setSubject  (findId(sInCol, binding, SUBJECT,   search.getSubject()));
                search.setPredicate(findId(pInCol, binding, PREDICATE, search.getPredicate()));
                search.setObject   (findId(oInCol, binding, OBJECT,    search.getObject()));
                it = triples.search(search);
            } finally {
                unlock(st);
            }
        }

        private long findId(int inCol, BatchBinding<?> binding, TripleComponentRole role,
                            long fallback) {
            if (inCol >= 0 && binding.get(inCol, view))
                return plain(dict, view, role);
            return fallback;
        }

        private boolean fill(HdtBatch dest, long limit) {
            long deadline = Timestamp.nextTick(LIMIT_TICKS);
            int rows = dest.rows;
            long[] rowSkel = (statePlain()&HAS_UNSET_OUT) != 0 ? this.rowSkel : null;
            boolean hasNext;
            for (int base = 0; (hasNext = it.hasNext()) && rows < limit; base += cols)  {
                TripleID t = it.next();
                dest.reserve(rows+1, 0);
                long[] arr = dest.arr;
                if (rowSkel != null)
                    System.arraycopy(rowSkel, 0, arr, base, cols);
                if (sOutCol >= 0) arr[base+sOutCol] = encode(t.getSubject(),   dictId, SUBJECT);
                if (pOutCol >= 0) arr[base+pOutCol] = encode(t.getPredicate(), dictId, PREDICATE);
                if (oOutCol >= 0) arr[base+oOutCol] = encode(t.getObject(),    dictId, OBJECT);
                ++rows;
                if ((rows&TIME_CHK_MASK) == TIME_CHK_MASK && Timestamp.nanoTime() > deadline)
                    break; // deadline expired
            }
            dest.dropCachedHashes();
            dest.rows = rows;
            return hasNext;
        }

        @Override protected int produceAndDeliver(int state) {
            long limit = (long)REQUESTED.getOpaque(this);
            if (limit <= 0)
                return state;
            int termState = state;
            HdtBatch b = TYPE.empty(asUnpooled(recycled), INIT_ROWS, cols, 0);
            recycled = null;
            int iLimit = (int)Math.min(MAX_BATCH, limit);
            if (!fill(b, iLimit))
                termState = COMPLETED;
            int rows = b.rows;
            if (rows > 0) {
                if ((long) REQUESTED.getAndAddRelease(this, -rows) > rows)
                    termState |= MUST_AWAKE;
                b = deliver(b);
            }
            recycled = asPooled(b);
            return termState;
        }
    }

    /* --- --- --- iterators --- --- --- */


    final class HdtIteratorBIt extends UnitaryBIt<HdtBatch> {

        private final IteratorTripleID it;
        private final byte v0Role;
        private final byte v1Role;
        private final byte v2Role;

        public HdtIteratorBIt(Vars vars, Term s, Term p, Term o, IteratorTripleID it) {
            super(TYPE, vars);
            acquireRef();
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
            try {
                super.cleanup(cause);
            } finally { releaseRef(); }
        }
    }

    public final class HdtBindingBIt extends BindingBIt<HdtBatch> {
        private final @Nullable Modifier modifier;
        private final TriplePattern right;
        private final long s, p, o;
        private final @Nullable BIt<HdtBatch> empty;
        private final Vars rightFreeVars;
        private final Dictionary dict;

        public HdtBindingBIt(ItBindQuery<HdtBatch> bq, Plan rightPlan) {
            super(bq, null);
            acquireRef();
            if (rightPlan instanceof Modifier m) {
                this.modifier = m;
                this.right = (TriplePattern) m.left();
            } else {
                this.modifier = null;
                this.right = (TriplePattern) rightPlan;
            }
            this.dict = hdt.getDictionary();
            this.s = plain(dict, right.s,   SUBJECT);
            this.p = plain(dict, right.p, PREDICATE);
            this.o = plain(dict, right.o,    OBJECT);
            this.rightFreeVars
                    = (modifier != null && modifier.filters.isEmpty() ? modifier : right)
                    .publicVars().minus(bq.bindings.vars());
            if (s == -1 || p == -1 || o == -1)
                empty = new EmptyBIt<>(TYPE, rightFreeVars);
            else
                empty = null;
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            super.cleanup(cause);
            releaseRef();
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
            BIt<HdtBatch> it = new HdtIteratorBIt(rightFreeVars, s, p, o, hdt.getTriples().search(query));
            if (modifier != null)
                it = modifier.executeFor(it, null, false);
            return it;
        }
    }

    private static final class LogProgressListener implements ProgressListener {
        private final String path;
        private final long interval = FSHdtProperties.mapProgressIntervalMs()*1_000_000L;
        private long lastMessage = Timestamp.nanoTime();
        private boolean logged;

        public LogProgressListener(String path) {
            this.path = path;
        }

        @Override public void notifyProgress(float level, String message) {
            long now = Timestamp.nanoTime();
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
