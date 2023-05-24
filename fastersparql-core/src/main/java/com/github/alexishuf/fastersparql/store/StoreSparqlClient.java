package com.github.alexishuf.fastersparql.store;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.SingletonBIt;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.base.UnitaryBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchFilter;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQueryType;
import com.github.alexishuf.fastersparql.fed.CardinalityEstimator;
import com.github.alexishuf.fastersparql.fed.Federation;
import com.github.alexishuf.fastersparql.fed.Source;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.fed.selectors.TrivialSelector;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.TripleRoleSet;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.Modifier;
import com.github.alexishuf.fastersparql.operators.plan.Operator;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.store.batch.IdTranslator;
import com.github.alexishuf.fastersparql.store.batch.StoreBatch;
import com.github.alexishuf.fastersparql.store.batch.StoreBatchType;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityCompositeDict;
import com.github.alexishuf.fastersparql.store.index.triples.Triples;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Type.VAR;
import static com.github.alexishuf.fastersparql.store.batch.IdTranslator.*;
import static com.github.alexishuf.fastersparql.store.index.dict.Dict.NOT_FOUND;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;

public class StoreSparqlClient extends AbstractSparqlClient {
    private static final Logger log = LoggerFactory.getLogger(StoreSparqlClient.class);
    private static final StoreBatchType TYPE = StoreBatchType.INSTANCE;
    private static final VarHandle REFS;
    static {
        try {
            REFS = MethodHandles.lookup().findVarHandle(StoreSparqlClient.class, "plainRefs", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final LocalityCompositeDict dict;
    private final int dictId;
    private final Triples spo, pso, ops;
    private final Federation federation;
    private final Estimator estimator = new Estimator();
    @SuppressWarnings("unused") private int plainRefs;
    private final int prefixesMask;
    private final SegmentRope[] prefixes;

    /* --- --- --- lifecycle --- --- --- */

    public StoreSparqlClient(SparqlEndpoint ep) {
        super(ep);
        int prefixesCap = (8*1024*1024)/(4/* SegmentRope ref */ + 32/* SegmentRope obj */);
        this.prefixesMask = -1 >>> Integer.numberOfLeadingZeros(prefixesCap);
        this.prefixes = new SegmentRope[prefixesMask+1];
        this.bindingAwareProtocol = true;
        Path dir = endpoint.asFile().toPath();
        boolean validate = FSProperties.storeClientValidate();
        log.info("Loading{} {}...", validate ? "/validating" : "", dir);
        if (!Files.isDirectory(dir))
            throw new FSException(ep, dir+" is not a dir, cannot open store");
        LocalityCompositeDict dict = null;
        int dictId = 0;
        Triples spo = null, pso = null, ops;
        try (var exec = newVirtualThreadPerTaskExecutor()) {
            var dictF = exec.submit(() -> {
                LocalityCompositeDict d = new LocalityCompositeDict(dir.resolve("strings"));
                if (validate)
                    d.validate();
                return d;
            });
            var spoF = exec.submit(() -> loadTriples(dir.resolve("spo"), validate));
            var psoF = exec.submit(() -> loadTriples(dir.resolve("pso"), validate));
            var opsF = exec.submit(() -> loadTriples(dir.resolve("ops"), validate));
            dict = dictF.get();
            dictId = IdTranslator.register(dict);
            spo = spoF.get();
            pso = psoF.get();
            ops = opsF.get();
        } catch (Throwable e) {
            if (dictId > 0) IdTranslator.deregister(dictId, dict);
            if (dict != null) dict.close();
            if (spo != null) spo.close();
            if (pso != null) pso.close();
            throw FSException.wrap(ep, e);
        }
        this.dictId = dictId;
        this.dict = dict;
        this.spo = spo;
        this.pso = pso;
        this.ops = ops;
        federation = new Federation(ep, null);
        var selector = new TrivialSelector(ep, Spec.EMPTY);
        federation.addSource(new Source(this, selector, estimator, Spec.EMPTY));
        REFS.setRelease(this, 1);
        log.info("Loaded{} {}...", validate ? "/validated" : "", dir);
    }

    private static Triples loadTriples(Path path, boolean validate) throws IOException {
        Triples t = new Triples(path);
        if (validate)
            t.validate();
        return t;
    }

    /** An {@link AutoCloseable} that ensures the {@link StoreSparqlClient} and the
     *  underlying storage remains open (i.e., {@link StoreSparqlClient#close()} will be
     *  silently delayed if there are unterminated {@link BIt}s or unclosed refs. */
    public class Ref implements AutoCloseable {
        private boolean closed;

        public Ref() { acquireRef(); }

        public StoreSparqlClient get() { return StoreSparqlClient.this; }

        @Override public void close() {
            if (closed) return;
            closed = true;
            releaseRef();
        }
    }

    /** Get a {@link Ref}, which will delay {@link StoreSparqlClient#close()} until after
     * the {@link Ref} itself is {@link Ref#close()}d. */
    public Ref liveRef() { return new Ref(); }

    private void acquireRef() {
        while (true) {
            int current = (int) REFS.getAcquire(this);
            if (current == 0)
                throw new IllegalStateException("Attempt to use closed SparqlClient "+this);
            if ((int)REFS.compareAndExchangeAcquire(this, current, current+1) == current)
                break;
            Thread.onSpinWait();
        }
    }

    private void releaseRef() {
        int refs = (int)REFS.getAndAddRelease(this, -1);
        if (refs == 1) {
            IdTranslator.deregister(dictId, dict);
            federation.close();
            dict.close();
            spo.close();
            pso.close();
            ops.close();
        } else if (refs < 0) {
            log.error("This or a previous releaseRef() was mismatched for {}",
                       this, new IllegalStateException());
            REFS.getAndAdd(this, 1);
        }
    }

    @Override public void close() {
        if ((int)REFS.getAcquire(this) > 0) // double close() is not an error
            releaseRef();
    }

    /* --- --- --- properties--- --- --- */

    void usesBindingAwareProtocol(boolean value) { bindingAwareProtocol = value; }
    public int dictId() { return dictId; }

    /* --- --- --- estimation --- --- --- */

    public int estimate(TriplePattern tp) { return estimator.estimate(tp, null); }

    private final class Estimator extends CardinalityEstimator {
        @Override public int estimate(TriplePattern t, @Nullable Binding binding) {
            var l = lookup(dictId);
            long s = t.s.type() == VAR ? 0 : l.find(t.s);
            long p = t.p.type() == VAR ? 0 : l.find(t.p);
            long o = t.o.type() == VAR ? 0 : l.find(t.o);
            long estimate = switch (binding == null ? t.varRoles() : t.varRoles(binding)) {
                case EMPTY        -> 1;
                case OBJ          -> spo.estimateValues(s, p);
                case PRE, PRE_OBJ -> spo.estimatePairs(s);
                case SUB          -> ops.estimateValues(o, p);
                case SUB_OBJ      -> pso.estimatePairs(p);
                case SUB_PRE      -> ops.estimatePairs(o);
                case SUB_PRE_OBJ  -> spo.triplesCount();
            };
            return (int)Math.min(Integer.MAX_VALUE, estimate);
        }
    }

    /* --- --- --- query --- --- --- */

    @Override public <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql) {
        BIt<StoreBatch> storeIt;
        Modifier m = sparql instanceof Modifier mod ? mod : null;
        TriplePattern tp = sparql instanceof TriplePattern t ? t
                : (m != null && m.left instanceof TriplePattern t ? t : null);
        if (tp != null) {
            var l = lookup(dictId);
            Vars tpVars = m != null && m.filters.isEmpty() ? m.publicVars() : tp.publicVars();
            storeIt = queryTP(tpVars, tp,
                    tp.s.type() == VAR ? NOT_FOUND : l.find(tp.s),
                    tp.p.type() == VAR ? NOT_FOUND : l.find(tp.p),
                    tp.o.type() == VAR ? NOT_FOUND : l.find(tp.o),
                    tp.varRoles());
            if (m != null)
                storeIt = m.executeFor(storeIt, null, false);
        } else {
            storeIt = federation.query(TYPE, sparql);
        }
        return batchType.convert(storeIt);
    }

    private BIt<StoreBatch> queryTP(Vars vars, TriplePattern t, long si, long pi, long oi,
                                    TripleRoleSet varRoles) {
        return switch (varRoles) {
            case EMPTY -> {
                if (spo.contains(si, pi, oi)) {
                    var b = TYPE.createSingleton(vars.size());
                    b.beginPut();
                    b.commitPut();
                    yield new SingletonBIt<>(b, TYPE, vars);
                }
                yield new EmptyBIt<>(TYPE, vars);
            }
            case OBJ         -> new  StoreValueBIt(vars, t.o,          spo. values(si, pi));
            case PRE         -> new StoreSubKeyBIt(vars, t.p,          spo.subKeys(si, oi));
            case PRE_OBJ     -> new   StorePairBIt(vars, t.p, t.o,     spo.  pairs(si));
            case SUB         -> new  StoreValueBIt(vars, t.s,          ops. values(oi, pi));
            case SUB_OBJ     -> new   StorePairBIt(vars, t.s, t.o,     pso.  pairs(pi));
            case SUB_PRE     -> new   StorePairBIt(vars, t.p, t.s,     ops.  pairs(oi));
            case SUB_PRE_OBJ -> new    StoreScanIt(vars, t.s, t.p, t.o);
        };
    }

    @Override
    public <B extends Batch<B>> BIt<B> query(BatchType<B> bt, SparqlQuery sparql,
                                             @Nullable BIt<B> bindings, @Nullable BindType type,
                                             Metrics.@Nullable JoinMetrics metrics) {
        if (bindings == null || bindings instanceof EmptyBIt<B>)
            return query(bt, sparql);
        else if (type == null)
            throw new NullPointerException("bindings != null, but type is null!");
        if (sparql.isGraph())
            throw new InvalidSparqlQueryType("query() method only takes SELECT/ASK queries");
        try {
            Plan q = new SparqlParser().parse(sparql);
            Vars outVars = type.resultVars(bindings.vars(), q.publicVars());
            TriplePattern tp = StoreBindingBIt.supportedTP(q);
            if (tp != null) {
                var l = lookup(dictId);
                long s = NOT_FOUND, p = NOT_FOUND, o = NOT_FOUND;
                boolean empty = (tp.s.type() != VAR && (s = l.find(tp.s)) == NOT_FOUND)
                             || (tp.p.type() != VAR && (p = l.find(tp.p)) == NOT_FOUND)
                             || (tp.o.type() != VAR && (o = l.find(tp.o)) == NOT_FOUND);
                if (empty) {
                    return new EmptyBIt<>(bt, outVars);
                }
                return new StoreBindingBIt<>(bindings, type, q, tp, s, p, o, outVars, metrics);
            } else {
                var it = tryModifiedBGP(bt, bindings, type, q, outVars, metrics);
                if (it != null)
                    return it;
            }
            return new ClientBindingBIt<>(bindings, type, this, q,  metrics);
        } catch (Throwable t) {
            throw FSException.wrap(endpoint, t);
        }
    }

    private <B extends Batch<B>>
    BIt<B> tryModifiedBGP(BatchType<B> bt, BIt<B> left, BindType outerType, Plan right,
                          Vars outVars, Metrics.@Nullable JoinMetrics metrics) {
        Plan join = right instanceof Modifier m ? m.left() : right;
        BindType innerType = join.type.bindType();
        if (innerType == null) return null;
        int n = join.opCount();
        for (int i = 0; i < n; i++) {
            if (!(join.op(i) instanceof TriplePattern)) return null;
        }
        Plan lastOp = join.op(n-1);
        if (right instanceof Modifier m) {
            Modifier m2 = (Modifier) m.copy();
            m2.replace(0, lastOp);
            m2.projection = m.publicVars();
            lastOp = m2;
        }
        var lookup = lookup(dictId);
        for (int i = 0; i < n; i++) {
            var tp = (TriplePattern)join.op(i);
            long s = NOT_FOUND, p = NOT_FOUND, o = NOT_FOUND;
            boolean empty = (tp.s.type() != VAR && (s = lookup.find(tp.s)) == NOT_FOUND)
                         || (tp.p.type() != VAR && (p = lookup.find(tp.p)) == NOT_FOUND)
                         || (tp.o.type() != VAR && (o = lookup.find(tp.o)) == NOT_FOUND);
            if (empty)
                return new EmptyBIt<>(bt, outVars);
            Metrics.JoinMetrics opMetrics = null;
            Vars vars;
            Plan tpOrMod;
            if (i == n-1) {
                opMetrics = metrics;
                vars = outVars;
                tpOrMod = lastOp;
            } else {
                vars = outerType.resultVars(left.vars(), tp.publicVars());
                tpOrMod = tp;
            }
            left = new StoreBindingBIt<>(left, i == 0 ? BindType.JOIN : innerType,
                                         tpOrMod, tp, s, p, o, vars, opMetrics);
        }
        return left;
    }

    /* --- --- --- iterators --- --- --- */

    static {
        assert Long.bitCount(IdTranslator.DICT_MASK) < 16 : "short is too small to store dictId!";
    }

    abstract class StoreIteratorBIt extends UnitaryBIt<StoreBatch> {
        /** Copy of {@link StoreSparqlClient#dictId} with better locality. */
        protected final short dictId;
        protected final byte term0Col, term1Col;
        public StoreIteratorBIt(Vars outVars, int term0Col, int term1Col) {
            super(TYPE, outVars);
            this.dictId = (short) StoreSparqlClient.this.dictId;
            if (term0Col > 127 || term1Col > 127)
                throw new IllegalArgumentException("too many columns");
            this.term0Col = (byte)term0Col;
            this.term1Col = (byte)term1Col;
            acquireRef();
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            try {
                super.cleanup(cause);
            } finally { releaseRef(); }
        }
    }

    final class StoreValueBIt extends StoreIteratorBIt {
        private final Triples.ValueIt vit;

        public StoreValueBIt(Vars vars, Term valueVar, Triples.ValueIt vit) {
            super(vars, vars.indexOf(valueVar), -1);
            this.vit = vit;
            if (vars.size() > 1) throw new IllegalArgumentException("Expected 1 var");
        }

        @Override protected boolean fetch(StoreBatch dest) {
            if (!vit.advance()) return false;
            dest.beginPut();
            if (term0Col >= 0)
                dest.putTerm(term0Col, source(vit.valueId, dictId));
            dest.commitPut();
            return true;
        }
    }

    final class StorePairBIt extends StoreIteratorBIt {
        private final Triples.PairIt pit;

        public StorePairBIt(Vars pubVars, Term subKeyVar, Term valueVar, Triples.PairIt pit) {
            super(pubVars, pubVars.indexOf(subKeyVar), pubVars.indexOf(valueVar));
            this.pit = pit;
        }

        @Override protected boolean fetch(StoreBatch dest) {
            if (!pit.advance()) return false;
            dest.beginPut();
            if (term0Col >= 0) dest.putTerm(term0Col, source(pit.subKeyId, dictId));
            if (term1Col >= 0) dest.putTerm(term1Col, source(pit.valueId,  dictId));
            dest.commitPut();
            return true;
        }
    }

    final class StoreSubKeyBIt extends StoreIteratorBIt {
        private final Triples.SubKeyIt sit;

        public StoreSubKeyBIt(Vars pubVars, Term subKeyVar, Triples.SubKeyIt sit) {
            super(pubVars, pubVars.indexOf(subKeyVar), -1);
            this.sit = sit;
        }

        @Override protected boolean fetch(StoreBatch dest) {
            if (!sit.advance()) return false;
            dest.beginPut();
            if (term0Col >= 0) dest.putTerm(term0Col, source(sit.subKeyId, dictId));
            dest.commitPut();
            return true;
        }
    }

    final class StoreScanIt extends UnitaryBIt<StoreBatch>  {
        private final short dictId;
        private final byte sCol, pCol, oCol;
        private final Triples.ScanIt sit;

        public StoreScanIt(Vars vars, Term sVar, Term pVar, Term oVar) {
            super(TYPE, vars);
            int sCol = vars.indexOf(sVar), pCol = vars.indexOf(pVar), oCol = vars.indexOf(oVar);
            if (sCol > 127 || pCol > 127 || oCol > 127)
                throw new IllegalArgumentException("Too many vars");
            this.dictId = (short) StoreSparqlClient.this.dictId;
            this.sCol = (byte)sCol;
            this.pCol = (byte)pCol;
            this.oCol = (byte)oCol;
            this.sit = spo.scan();
            acquireRef();
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            try {
                super.cleanup(cause);
            } finally { releaseRef(); }
        }

        @Override protected boolean fetch(StoreBatch dest) {
            if (!sit.advance()) return false;
            dest.beginPut();
            if (sCol >= 0) dest.putTerm(sCol, source(sit.keyId,    dictId));
            if (pCol >= 0) dest.putTerm(pCol, source(sit.subKeyId, dictId));
            if (oCol >= 0) dest.putTerm(oCol, source(sit.valueId,  dictId));
            dest.commitPut();
            return true;
        }
    }

    final class StoreBindingBIt<B extends Batch<B>> extends AbstractBIt<B> {
        private final BindType bindType;
        private final BIt<B> left;
        private Object rIt;
        private B lb;
        private int lr;
        private B rb;
        private @Nullable B fb;
        private final @Nullable BatchMerger<B> merger;
        private final TripleRoleSet tpFreeRoles;
        private final Metrics.@Nullable JoinMetrics joinMetrics;
        private final long s, p, o;
        private final byte sLeftCol, pLeftCol, oLeftCol;
        private final byte kCol, skCol, vCol, tpFreeCols;
        private final short dictId, prbCols;
        private boolean rEnd;
        private final boolean rightSingleRow;
        private final TwoSegmentRope ropeView = new TwoSegmentRope();
        private ByteRope localCopy;
        private final @Nullable BatchMerger<B> preFilterMerger;
        private final @Nullable BatchFilter<B> rightFilter;

        public static TriplePattern supportedTP(Plan right) {
            if (right instanceof TriplePattern t) return t;
            if (right instanceof Modifier m) {
                Plan c = m.left();
                if (c instanceof TriplePattern t) return t;
                if (c.right == null && c.left instanceof TriplePattern t
                        && (c.type == Operator.JOIN || c.type == Operator.UNION))
                    return t;
            }
            return null;
        }

        public StoreBindingBIt(BIt<B> left, BindType type, Plan right, TriplePattern tp,
                               long s, long p, long o, Vars projection,
                               Metrics.@Nullable JoinMetrics metrics) {
            super(left.batchType(), projection);
            this.bindType = type;
            this.left = left;
            Vars leftVars = left.vars();
            Vars tpFree = tp.publicVars().minus(leftVars), procRightFree;
            var m = right instanceof Modifier mod ? mod : null;
            if (m != null && (!m.filters.isEmpty()
                    || (m.limit > 1 && m.limit < Long.MAX_VALUE)
                    || m.offset > 0
                    || m.distinctCapacity > 0)) {
                boolean needsLeftVars = false;
                for (Expr expr : m.filters) {
                    if (!tpFree.containsAll(expr.vars())) { needsLeftVars = true; break; }
                }
                if (needsLeftVars) {
                    procRightFree = leftVars.union(tpFree);
                    preFilterMerger = batchType.merger(procRightFree, leftVars, tpFree);
                    fb = batchType.create(PREFERRED_MIN_BATCH, procRightFree.size(), PREFERRED_MIN_BATCH*32);
                } else {
                    procRightFree = tpFree;
                    preFilterMerger = null;
                }
                rightFilter = (BatchFilter<B>) m.processorFor(batchType, procRightFree, null, false);
            } else {
                procRightFree = tpFree;
                m = null;
                preFilterMerger = null;
                rightFilter = null;
            }
            if (procRightFree.size() > Short.MAX_VALUE)
                throw new IllegalArgumentException("Too many (>32767) left+right columns");
            this.prbCols = (short) procRightFree.size();
            this.rightSingleRow = switch (bindType) {
                case JOIN,LEFT_JOIN          -> m != null && m.limit == 1;
                case EXISTS,NOT_EXISTS,MINUS -> true;
            };
            this.merger = !type.isJoin() || procRightFree.equals(projection) ? null
                        : batchType.merger(projection, leftVars, procRightFree);
            this.lb = batchType.create(PREFERRED_MIN_BATCH, leftVars.size(), PREFERRED_MIN_BATCH*32);
            this.dictId = (short)StoreSparqlClient.this.dictId;
            int sLeftCol = leftVars.indexOf(tp.s);
            int pLeftCol = leftVars.indexOf(tp.p);
            int oLeftCol = leftVars.indexOf(tp.o);
            if ((sLeftCol|pLeftCol|oLeftCol) > 127)
                throw new IllegalArgumentException("binding column >127");
            this.sLeftCol = (byte) sLeftCol;
            this.pLeftCol = (byte) pLeftCol;
            this.oLeftCol = (byte) oLeftCol;
            int tpFreeCols = tpFree.size();
            this.tpFreeCols = (byte) tpFreeCols;
            this.rb = batchType.create(BIt.PREFERRED_MIN_BATCH, tpFreeCols, 0);
            byte sCol = (byte)tpFree.indexOf(tp.s);
            byte pCol = (byte)tpFree.indexOf(tp.p);
            byte oCol = (byte)tpFree.indexOf(tp.o);
            this.tpFreeRoles = TripleRoleSet.fromBitset(
                    (s == NOT_FOUND && sLeftCol < 0 ? 0x4 : 0x0) |
                    (p == NOT_FOUND && pLeftCol < 0 ? 0x2 : 0x0) |
                    (o == NOT_FOUND && oLeftCol < 0 ? 0x1 : 0x0) );
            byte kCol = -1, skCol = -1, vCol = -1;
            switch (tpFreeRoles) {
                case EMPTY -> rIt = TRUE;
                case SUB -> {
                    rIt = ops.makeValuesIt();
                    vCol = sCol;
                }
                case OBJ -> {
                    rIt = spo.makeValuesIt();
                    vCol = oCol;
                }
                case PRE -> {
                    rIt = spo.makeSubKeyIt();
                    skCol = pCol;
                }
                case PRE_OBJ -> {
                    rIt = spo.makePairIt();
                    skCol = pCol;
                    vCol = oCol;
                }
                case SUB_OBJ -> {
                    rIt = pso.makePairIt();
                    skCol = sCol;
                    vCol = oCol;
                }
                case SUB_PRE -> {
                    rIt = ops.makePairIt();
                    skCol = pCol;
                    vCol = sCol;
                }
                case SUB_PRE_OBJ -> {
                    rIt = spo.scan();
                    kCol = sCol;
                    skCol = pCol;
                    vCol = oCol;
                }
            }
            this.kCol  = kCol;
            this.skCol = skCol;
            this.vCol  = vCol;
            this.s = s;
            this.p = p;
            this.o = o;
            this.rEnd = true;
            this.metrics = metrics;
            this.joinMetrics = metrics;
            acquireRef();
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            try {
                super.cleanup(cause);
            } finally { releaseRef(); }
        }

        @Override public @Nullable B nextBatch(@Nullable B b) {
            if (lb == null) return null; // already exhausted
            try {
                long startNs = needsStartTime ? Timestamp.nanoTime() : ORIGIN;
                long innerDeadline = rightSingleRow ? ORIGIN-1 : startNs+minWaitNs;
                b = getBatch(b);
                do {
                    boolean rEmpty = rEnd;
                    if (rEmpty) {
                        if (++lr >= lb.rows) {
                            lr = 0;
                            lb = left.nextBatch(lb);
                            if (lb == null) break; // reached end
                        }
                        rebind(lb, lr);
                    }
                    // fill rb with values from bound rIt
                    rb.clear(tpFreeCols);
                    switch (tpFreeRoles) {
                        case EMPTY -> {
                            if (rIt == TRUE) {
                                rb.beginPut();
                                rb.commitPut();
                                rIt = FALSE;
                            }
                        }
                        case SUB_PRE_OBJ -> {
                            Triples.ScanIt it = (Triples.ScanIt) rIt;
                            while (it.advance()) {
                                putRow(it.keyId, it.subKeyId, it.valueId);
                                if (Timestamp.nanoTime() > innerDeadline) break;
                            }
                        }
                        case SUB,OBJ -> {
                            for (var it = (Triples.ValueIt) rIt; it.advance(); ) {
                                putRow(0, 0, it.valueId);
                                if (Timestamp.nanoTime() > innerDeadline) break;
                            }
                        }
                        case PRE -> {
                            for (var it = (Triples.SubKeyIt)rIt; it.advance(); ) {
                                putRow(0, it.subKeyId, 0);
                                if (Timestamp.nanoTime() > innerDeadline) break;
                            }
                        }
                        case SUB_PRE,SUB_OBJ,PRE_OBJ -> {
                            for (var it = (Triples.PairIt)rIt; it.advance(); ) {
                                putRow(0, it.subKeyId, it.valueId);
                                if (Timestamp.nanoTime() > innerDeadline) break;
                            }
                        }
                    }
                    rEnd = rb.rows == 0;
                    B prb = rb;
                    if (!rEnd && rightFilter != null) {
                        if (preFilterMerger != null) {//noinspection DataFlowIssue fb != null
                            fb.clear(prbCols);
                            prb = fb = preFilterMerger.merge(fb, lb, lr, rb);
                        }
                        prb = rightFilter.filterInPlace(prb);
                        if (prb.rows == 0)
                            continue;
                    }
                    boolean pub = switch (bindType) {
                        case JOIN,EXISTS      ->  !rEnd;
                        case NOT_EXISTS,MINUS ->   rEnd;
                        case LEFT_JOIN        -> { rEmpty &= rEnd; yield !rEnd || rEmpty; }
                    };
                    if (pub) {
                        switch (bindType) {
                            case JOIN,LEFT_JOIN -> {
                                if (merger == null) {
                                    if (prb == fb) fb = b;
                                    else           rb = b;
                                    b = prb;
                                } else {
                                    b = merger.merge(b, lb, lr, prb);
                                }
                            }
                            case EXISTS,NOT_EXISTS,MINUS -> {
                                b.putRow(lb, lr);
                                rEnd = true;
                            }
                        }
                    }
                } while (readyInNanos(b.rows, startNs) > 0);
                if (b.rows == 0) b = handleEmptyBatch(b);
                else             onBatch(b);
            } catch (Throwable t) {
                lb = null; // signal exhaustion
                onTermination(t);
                throw t;
            }
            return b;
        }

        private B handleEmptyBatch(B batch) {
            batchType.recycle(recycle(batch));
            onTermination(null);
            return null;
        }

        private void putRow(long kId, long skId, long vId) {
            rb.beginPut();
            if (rb instanceof StoreBatch sb) {
                if ( kCol >= 0) sb.putTerm(kCol,  source( kId, dictId));
                if (skCol >= 0) sb.putTerm(skCol, source(skId, dictId));
                if ( vCol >= 0) sb.putTerm(vCol,  source( vId, dictId));
            } else {
                var lookup = lookup(dictId);
                if ( kCol >= 0) convertTerm(rb, kCol,   kId, lookup);
                if (skCol >= 0) convertTerm(rb, skCol, skId, lookup);
                if ( vCol >= 0) convertTerm(rb, vCol,   vId, lookup);
            }
            rb.commitPut();
        }

        private void convertTerm(B dest, byte col, long id, LocalityCompositeDict.Lookup lookup) {
            TwoSegmentRope t = lookup.get(id);
            if (t == null) return;
            byte fst = t.get(0);
            SegmentRope sh = switch (fst) {
                case '"' -> SHARED_ROPES.internDatatypeOf(t, 0, t.len);
                case '<' -> {
                    if (t.fstLen == 0 || t.sndLen == 0) yield ByteRope.EMPTY;
                    int slot = (int)(t.fstOff & prefixesMask);
                    var cached = prefixes[slot];
                    if (cached == null || cached.offset != t.fstOff || cached.segment != t.fst)
                        prefixes[slot] = cached = new SegmentRope(t.fst, t.fstOff, t.fstLen);
                    yield cached;
                }
                case '_' -> ByteRope.EMPTY;
                default -> throw new IllegalArgumentException("Not an RDF term");
            };
            MemorySegment local;
            long localOff;
            int localLen;
            if (sh.len == t.sndLen) {
                local =  t.fst;
                localOff = t.fstOff;
                localLen = t.fstLen;
            } else if (sh.len == t.fstLen) {
                local = t.snd;
                localOff = t.sndOff;
                localLen = t.sndLen;
            } else {
                if (localCopy == null) localCopy = new ByteRope(t.len);
                else                   localCopy.clear();
                if (fst == '"') localCopy.append(t, 0, t.len-sh.len);
                else            localCopy.append(t, sh.len, t.len);
                local = localCopy.segment;
                localOff = 0;
                localLen = localCopy.len;
            }
            dest.putTerm(col, sh, local, localOff, localLen, fst == '"');
        }

        private void rebind(B lb, int lr) {
            long s, p, o;
            if (lb instanceof StoreBatch sb) {
                s = sLeftCol < 0 ? this.s : translate(sb.id(lr, sLeftCol), dictId);
                p = pLeftCol < 0 ? this.p : translate(sb.id(lr, pLeftCol), dictId);
                o = oLeftCol < 0 ? this.o : translate(sb.id(lr, oLeftCol), dictId);
            } else {
                s = this.s;
                p = this.p;
                o = this.o;
                var l = lookup(dictId);
                if (sLeftCol >= 0 && lb.getRopeView(lr, sLeftCol, ropeView))
                    s = l.find(ropeView);
                if (pLeftCol >= 0 && lb.getRopeView(lr, pLeftCol, ropeView))
                    p = l.find(ropeView);
                if (oLeftCol >= 0 && lb.getRopeView(lr, oLeftCol, ropeView))
                    o = l.find(ropeView);
            }
            switch (tpFreeRoles) {
                case EMPTY       -> rIt = spo.contains(s, p, o);
                case SUB_PRE_OBJ ->    spo.scan(      (Triples.ScanIt)  rIt);
                case OBJ         ->  spo.values(s, p, (Triples.ValueIt) rIt);
                case PRE         -> spo.subKeys(s, o, (Triples.SubKeyIt)rIt);
                case PRE_OBJ     ->   spo.pairs(s,    (Triples.PairIt)  rIt);
                case SUB         ->  ops.values(o, p, (Triples.ValueIt) rIt);
                case SUB_OBJ     ->   pso.pairs(p,    (Triples.PairIt)  rIt);
                case SUB_PRE     ->   ops.pairs(o,    (Triples.PairIt)  rIt);
            }
            if (rightFilter != null) rightFilter.reset();
            if (joinMetrics != null) joinMetrics.beginBinding();
        }
    }
}
