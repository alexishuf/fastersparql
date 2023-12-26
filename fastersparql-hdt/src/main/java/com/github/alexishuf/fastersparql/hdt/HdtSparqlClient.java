package com.github.alexishuf.fastersparql.hdt;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.base.UnitaryBIt;
import com.github.alexishuf.fastersparql.batch.operators.BindingBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.ItBindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.async.EmitterService;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;
import com.github.alexishuf.fastersparql.fed.CardinalityEstimatorProvider;
import com.github.alexishuf.fastersparql.fed.SingletonFederator;
import com.github.alexishuf.fastersparql.hdt.batch.HdtBatch;
import com.github.alexishuf.fastersparql.hdt.batch.IdAccess;
import com.github.alexishuf.fastersparql.hdt.cardinality.HdtCardinalityEstimator;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.Modifier;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
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

import static com.github.alexishuf.fastersparql.hdt.FSHdtProperties.estimatorPeek;
import static com.github.alexishuf.fastersparql.hdt.batch.HdtBatchType.HDT;
import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.*;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static org.rdfhdt.hdt.enums.TripleComponentRole.*;

public class HdtSparqlClient extends AbstractSparqlClient implements CardinalityEstimatorProvider {
    private static final Logger log = LoggerFactory.getLogger(HdtSparqlClient.class);

    private final HDT hdt;
    final int dictId;
    private final SingletonFederator federator;
    private final HdtCardinalityEstimator estimator;

    public HdtSparqlClient(SparqlEndpoint ep) {
        super(ep);
        this.bindingAwareProtocol = true;
        this.cheapestDistinct = DistinctType.WEAK;
        this.localInProcess = true;
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
        federator = new SingletonFederator(this, HDT, estimator);
    }

    @Override public SparqlClient.Guard retain() { return new RefGuard(); }

    @Override protected void doClose() {
        if (ThreadJournal.ENABLED)
            ThreadJournal.journal("Closing dictId=", dictId, "ep=", endpoint);
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
            hdtIt = federator.execute(HDT, plan);
        }
        return bt.convert(hdtIt);
    }

    @Override protected <B extends Batch<B>> Emitter<B>
    doEmit(BatchType<B> bt, SparqlQuery sparql, Vars rebindHint) {
        var plan = SparqlParser.parse(sparql);
        Emitter<HdtBatch> hdtEm;
        Modifier m = plan instanceof Modifier mod ? mod : null;
        if ((m == null ? plan : m.left) instanceof TriplePattern tp) {
            Vars vars = (m != null && m.filters.isEmpty() ? m : tp).publicVars();
            hdtEm = new TPEmitter(tp, vars);
            if (m != null)
                hdtEm = m.processed(hdtEm);
        } else {
            hdtEm = federator.emit(HDT, plan, rebindHint);
        }
        return bt.convert(hdtEm);
    }

    private BIt<HdtBatch> queryTP(Vars vars, TriplePattern tp) {
        long s, p, o;
        BIt<HdtBatch> it;
        var dict = hdt.getDictionary();
        if (       (s = plain(dict, tp.s,   SUBJECT)) == NOT_FOUND
                || (p = plain(dict, tp.p, PREDICATE)) == NOT_FOUND
                || (o = plain(dict, tp.o,    OBJECT)) == NOT_FOUND) {
            it = new EmptyBIt<>(HDT, vars);
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
        if (bq.bindings.batchType() == HDT && (q instanceof TriplePattern
                || (q instanceof Modifier m && m.left instanceof TriplePattern))) {
            //noinspection unchecked
            return (BIt<B>) new HdtBindingBIt((ItBindQuery<HdtBatch>) bq, q);
        }
        return new ClientBindingBIt<>(bq, this);
    }

    /* --- --- --- emitters --- --- --- */

    final class TPEmitter extends com.github.alexishuf.fastersparql.emit.async.TaskEmitter<HdtBatch> {
        private static final BatchBinding EMPTY_BINDING;
        static {
            EMPTY_BINDING = new BatchBinding(Vars.EMPTY);
            var b = HDT.create(0);
            b.beginPut();
            b.commitPut();
            EMPTY_BINDING.attach(b, 0);
        }

        private static final int LIMIT_TICKS = 1;
        private static final int TIME_CHK_MASK = 0x1f;
        private static final int HAS_UNSET_OUT = 0x01000000;
        private static final Flags FLAGS = TASK_FLAGS.toBuilder()
                .flag(HAS_UNSET_OUT, "UNSET_OUT").build();

        private IteratorTripleID it;
        private final short dictId = (short)HdtSparqlClient.this.dictId;
        private final byte cols;
        private final byte sOutCol, pOutCol, oOutCol;
        private boolean retry;
        private long[] rowSkel;
        // ---------- fields below this line are accessed only on construction/rebind()
        private @MonotonicNonNull Vars lastBindingVars;
        private byte sInCol, pInCol, oInCol;
        private final TriplePattern tp;
        private final Dictionary dict = HdtSparqlClient.this.hdt.getDictionary();
        private final Triples triples = HdtSparqlClient.this.hdt.getTriples();
        private Term view = Term.pooledMutable();
        private int @Nullable[] skelCol2InCol;
        private final TripleID search = new TripleID();
        private final Vars bindableVars;

        public TPEmitter(TriplePattern tp, Vars outVars) {
            super(HDT, outVars, EmitterService.EMITTER_SVC, RR_WORKER, CREATED, FLAGS);
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
            this.bindableVars = tp.allVars();
            Arrays.fill(rowSkel = ArrayPool.longsAtLeast(cols), 0L);
            if (!tp.publicVars().containsAll(outVars))
                setFlagsRelease(statePlain(), HAS_UNSET_OUT);
            search.setSubject  (plain(dict, tp.s, SUBJECT));
            search.setPredicate(plain(dict, tp.p, PREDICATE));
            search.setObject   (plain(dict, tp.o, OBJECT));
            rebind(BatchBinding.ofEmpty(HDT));
            if (stats != null)
                stats.rebinds = 0;
            it = triples.search(search);
            if (ResultJournal.ENABLED)
                ResultJournal.initEmitter(this, vars);
            acquireRef();
        }

        @Override public String toString() {
            return super.toString()+'('+tp+')';
        }

        @Override protected void appendToSimpleLabel(StringBuilder out) {
            String uri = endpoint.uri();
            int begin = uri.lastIndexOf('/');
            String file = begin < 0 ? uri : uri.substring(begin);
            out.append('\n').append(tp).append(file);
        }

        @Override protected void doRelease() {
            try {
                ArrayPool.LONG.offer(rowSkel, rowSkel.length);
                if (skelCol2InCol != null)
                    skelCol2InCol = ArrayPool.INT.offer(skelCol2InCol, skelCol2InCol.length);
                rowSkel = ArrayPool.EMPTY_LONG;
                view.recycle();
                view = Term.EMPTY_STRING;
                releaseRef();
            } finally {
                super.doRelease();
            }
        }

        private int bindingsVarsChanged(int state, Vars bVars) {
            lastBindingVars = bVars;
            int sInCol = bVars.indexOf(tp.s);
            int pInCol = bVars.indexOf(tp.p);
            int oInCol = bVars.indexOf(tp.o);
            if (sInCol > 0x7f || pInCol > 0x7f || oInCol > 0x7f)
                throw new IllegalArgumentException("Too many binding vars");
            this.sInCol = (byte)sInCol;
            this.pInCol = (byte)pInCol;
            this.oInCol = (byte)oInCol;
            var tpVars = tp.publicVars();
            boolean hasUnset = false;
            int cols = vars.size();
            for (int c = 0; c < cols && !hasUnset; c++) {
                var outVar = vars.get(c);
                hasUnset = !tpVars.contains(outVar) || bVars.contains(outVar);
            }
            if (hasUnset) {
                if (skelCol2InCol == null)
                    skelCol2InCol = ArrayPool.intsAtLeast(cols);
                for (int c = 0; c < cols; c++)
                    skelCol2InCol[c] = bVars.indexOf(vars.get(c));
                return setFlagsRelease(state, HAS_UNSET_OUT);
            } else {
                return clearFlagsRelease(state, HAS_UNSET_OUT);
            }
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            Vars bVars = binding.vars;
            if (EmitterStats.ENABLED  && stats != null)
                stats.onRebind(binding);
            if (ResultJournal.ENABLED)
                ResultJournal.rebindEmitter(this, binding);
            int st = resetForRebind(0, LOCKED_MASK);
            try {
                if (!bVars.equals(lastBindingVars))
                    st = bindingsVarsChanged(st, bVars);
                if ((st&HAS_UNSET_OUT) != 0) {
                    int[] skelCol2InCol = this.skelCol2InCol;
                    if (skelCol2InCol != null) {
                        for (int c = 0; c < cols; c++) {
                            int bc = skelCol2InCol[c];
                            rowSkel[c] = bc < 0 || !binding.get(bc, view) ? NOT_FOUND
                                       : IdAccess.encode(dictId, dict, view);
                        }
                    }
                }
                search.setSubject  (findId(sInCol, binding, SUBJECT,   search.getSubject()));
                search.setPredicate(findId(pInCol, binding, PREDICATE, search.getPredicate()));
                search.setObject   (findId(oInCol, binding, OBJECT,    search.getObject()));
                it = triples.search(search);
            } finally {
                unlock(st);
            }
        }

        @Override public Vars bindableVars() { return bindableVars; }

        private long findId(int inCol, BatchBinding binding, TripleComponentRole role,
                            long fallback) {
            if (inCol >= 0 && binding.get(inCol, view))
                return plain(dict, view, role);
            return fallback;
        }

        private HdtBatch fill(long limit) {
            long deadline = Timestamp.nextTick(LIMIT_TICKS);
            HdtBatch dst = HDT.createForThread(threadId, cols);
            long[] arr = dst.arr;
            limit = Math.min(dst.termsCapacity/Math.max(1, cols), limit);
            short rows = 0;
            long[] rowSkel = (statePlain()&HAS_UNSET_OUT) != 0 ? this.rowSkel : null;
            for (int base = 0; (retry = it.hasNext()) && rows < limit; base += cols)  {
                if (rowSkel  != null) arraycopy(rowSkel, 0, arr, base, cols);
                var t = it.next();
                if (sOutCol >= 0) arr[base+sOutCol] = encode(t.getSubject(),   dictId, SUBJECT);
                if (pOutCol >= 0) arr[base+pOutCol] = encode(t.getPredicate(), dictId, PREDICATE);
                if (oOutCol >= 0) arr[base+oOutCol] = encode(t.getObject(),    dictId, OBJECT);
                ++rows;
                if ((rows&TIME_CHK_MASK) == TIME_CHK_MASK && Timestamp.nanoTime() > deadline)
                    break; // deadline expired
            }
            dst.dropCachedHashes();
            dst.rows = rows;
            return dst;
        }

        @Override protected int produceAndDeliver(int state) {
            long limit = requested();
            if (limit <= 0)
                return state;
            int termState = state;
            var b = fill(limit);
            if (!retry)
                termState = COMPLETED;
            int rows = b.rows; // fill() only fills up to b.termsCapacity
            if (rows > 0)
                b = deliver(b);
            HDT.recycleForThread(threadId, b);
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
            super(HDT, vars);
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

        @Override protected HdtBatch fetch(HdtBatch dest) {
            if (it.hasNext()) {
                TripleID t = it.next();
                dest.beginPut();
                int dictId = HdtSparqlClient.this.dictId;
                putRole(0, dest, v0Role, t, dictId);
                putRole(1, dest, v1Role, t, dictId);
                putRole(2, dest, v2Role, t, dictId);
                dest.commitPut();
            } else {
                exhausted = true;
            }
            return dest;
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
                empty = new EmptyBIt<>(HDT, rightFreeVars);
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

        private long bind(Term original, long sourcedId, TripleComponentRole role,
                          Term offer) {
            if (offer == original) return sourcedId;
            if (offer ==     null) return NOT_FOUND;
            return plain(dict, offer, role);
        }

        @Override protected BIt<HdtBatch> bind(BatchBinding binding) {
            if (empty != null) return empty;
            TripleID query;
            Term s = binding.getIf(right.s);
            Term p = binding.getIf(right.p);
            Term o = binding.getIf(right.o);
            Vars bVars = binding.vars;
            var bb = binding.batch;
            if (bb instanceof HdtBatch hb) {
                long[] batch = hb.arr;
                int base = binding.row*bb.cols;
                query = new TripleID(bind(right.s, this.s, SUBJECT,   batch, base, bVars),
                                     bind(right.p, this.p, PREDICATE, batch, base, bVars),
                                     bind(right.o, this.o, OBJECT,    batch, base, bVars));

            } else if (bb == null) {
                query = new TripleID(this.s, this.p, this.o);
            } else {
                query = new TripleID(bind(right.s, this.s, SUBJECT,   s),
                                     bind(right.p, this.p, PREDICATE, p),
                                     bind(right.o, this.o, OBJECT,    o));
            }
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
