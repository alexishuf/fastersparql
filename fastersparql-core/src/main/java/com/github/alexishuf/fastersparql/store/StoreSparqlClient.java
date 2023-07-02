package com.github.alexishuf.fastersparql.store;

import com.github.alexishuf.fastersparql.FS;
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
import com.github.alexishuf.fastersparql.client.BindQuery;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQueryType;
import com.github.alexishuf.fastersparql.fed.CardinalityEstimator;
import com.github.alexishuf.fastersparql.fed.CardinalityEstimatorProvider;
import com.github.alexishuf.fastersparql.fed.Optimizer;
import com.github.alexishuf.fastersparql.fed.SingletonFederator;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.TripleRoleSet;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.store.batch.IdTranslator;
import com.github.alexishuf.fastersparql.store.batch.StoreBatch;
import com.github.alexishuf.fastersparql.store.batch.StoreBatchType;
import com.github.alexishuf.fastersparql.store.index.dict.LexIt;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityCompositeDict;
import com.github.alexishuf.fastersparql.store.index.triples.Triples;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.github.alexishuf.fastersparql.FSProperties.dedupCapacity;
import static com.github.alexishuf.fastersparql.model.TripleRoleSet.EMPTY;
import static com.github.alexishuf.fastersparql.model.TripleRoleSet.PRE;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.operators.plan.Operator.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Type.VAR;
import static com.github.alexishuf.fastersparql.store.batch.IdTranslator.*;
import static com.github.alexishuf.fastersparql.store.index.dict.Dict.NOT_FOUND;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.copyOfRange;
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;

public class StoreSparqlClient extends AbstractSparqlClient
                               implements CardinalityEstimatorProvider {
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
    private static final int PREFIXES_MASK = -1 >>> Integer.numberOfLeadingZeros(
            (8*1024*1024)/(4/* SegmentRope ref */ + 32/* SegmentRope obj */));
    private static final LIFOPool<SegmentRope[]> PREFIXES_POOL
            = new LIFOPool<>(SegmentRope[].class, 16);

    private final LocalityCompositeDict dict;
    private final int dictId;
    private final Triples spo, pso, ops;
    private final SingletonFederator federator;
    @SuppressWarnings("unused") private int plainRefs;
    private final SegmentRope[] prefixes;

    /* --- --- --- lifecycle --- --- --- */

    public StoreSparqlClient(SparqlEndpoint ep) {
        super(ep);
        SegmentRope[] prefixes = PREFIXES_POOL.get();
        this.prefixes = prefixes == null ? new SegmentRope[PREFIXES_MASK+1] : prefixes;
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
        this.federator = new StoreSingletonFederator(this);
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
     *  silently delayed if there are unterminated {@link BIt}s or unclosed refs). */
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
            dict.close();
            spo.close();
            pso.close();
            ops.close();
            PREFIXES_POOL.offer(prefixes);
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

    /* --- --- --- estimation & optimization --- --- --- */

    public CardinalityEstimator estimator() { return federator; }

    public int estimate(TriplePattern tp) { return federator.estimate(tp, null); }

    private static final class StoreSingletonFederator extends SingletonFederator {
        private final int dictId;
        private final Triples spo, pso, ops;
        private final int avgS, avgP, avgO, avgSP, avgSO, avgPO;
        private final float invAvgSP, invAvgSO, invAvgPO;

        public StoreSingletonFederator(StoreSparqlClient parent) {
            super(parent, StoreSparqlClient.TYPE);
            this.spo = parent.spo;
            this.pso = parent.pso;
            this.ops = parent.ops;
            this.dictId = parent.dictId;
            int[] avg = new int[3];
            Thread sThread = Thread.startVirtualThread(() -> avg[0] = computeAvgPairs(spo));
            Thread pThread = Thread.startVirtualThread(() -> avg[1] = computeAvgPairs(pso));
            Thread oThread = Thread.startVirtualThread(() -> avg[2] = computeAvgPairs(ops));
            Async.uninterruptibleJoin(sThread);
            Async.uninterruptibleJoin(pThread);
            Async.uninterruptibleJoin(oThread);
            this.avgS = avg[0];
            this.avgP = avg[1];
            this.avgO = avg[2];
            avgSP = (int)Math.min(Integer.MAX_VALUE, 1+ops.triplesCount()/(float)ops.keysCount());
            avgSO = (int)Math.min(Integer.MAX_VALUE, 1+pso.triplesCount()/(float)pso.keysCount());
            avgPO = (int)Math.min(Integer.MAX_VALUE, 1+spo.triplesCount()/(float)spo.keysCount());
            invAvgSP = 1/(float)avgSP;
            invAvgSO = 1/(float)avgSO;
            invAvgPO = 1/(float)avgPO;
        }

        private int computeAvgPairs(Triples triples) {
            long sum = 0;
            for (long k = triples.firstKey(), end = k+triples.keysCount(); k < end; k++)
                sum += triples.estimatePairs(k);
            return (int)Math.min(Integer.MAX_VALUE, (long)((float)sum/triples.keysCount()));
        }

        @Override public int estimate(TriplePattern t, @Nullable Binding binding) {
            var l = lookup(dictId);
            long s = t.s.type() == VAR ? 0 : l.find(t.s);
            long p = t.p.type() == VAR ? 0 : l.find(t.p);
            long o = t.o.type() == VAR ? 0 : l.find(t.o);
            var vars = binding == null ? t.varRoles() : t.varRoles(binding);
            if (vars == EMPTY)
                return 1; // short circuit for ask queries

            // we cannot delegate estimation if we have a GROUND dummy term. Doing so would get
            // estimate == 0 even for expensive queries. When returning an avg*, field is not
            // enough, estimation relies on weighing avgS/avgP/avgO by an estimatePairs() of
            // another non-dummy grounded term. The following switch is not the prettiest, but
            // it avoids having 7 call sites to estimatePairs(). Too many call sites are a slight
            // perf issue as the JIT will give up inlining or will output huge code
            var dummies = TripleRoleSet.fromBitset((t.s == GROUND ? 0x4 : 0x0)
                                                 | (t.p == GROUND ? 0x2 : 0x0)
                                                 | (t.o == GROUND ? 0x1 : 0x0));
            Triples epObj = null;
            long epKey = 0;
            float dummyWeight = 1;
            switch (dummies) {
                case SUB -> {
                    switch (vars) {
                        case PRE     -> { epObj = ops; epKey = o; dummyWeight = invAvgSP; }
                        case OBJ     -> { epObj = pso; epKey = p; dummyWeight = invAvgSO; }
                        case PRE_OBJ -> { return avgPO; }
                    }
                }
                case PRE -> {
                    switch (vars) {
                        case SUB     -> { epObj = ops; epKey = o; dummyWeight = invAvgSP; }
                        case OBJ     -> { epObj = spo; epKey = s; dummyWeight = invAvgPO; }
                        case SUB_OBJ -> { return avgSO; }
                    }
                }
                case OBJ -> {
                    switch (vars) {
                        case SUB -> { epObj = pso; epKey = p; dummyWeight = invAvgSO; }
                        case PRE -> { epObj = spo; epKey = s; dummyWeight = invAvgPO; }
                        case SUB_PRE -> { return avgSP; }
                    }
                }
                case SUB_PRE -> { return avgO; }
                case SUB_OBJ -> { return avgP; }
                case PRE_OBJ -> { return avgS; }
                case EMPTY -> {
                    if (vars == PRE) { epObj = ops; epKey = o; }
                }
            }
            if (epObj != null)
                dummyWeight *= epObj.estimatePairs(epKey);

            // at this point, avg* fields and 1 are not answers, delegate to Triples or
            // approximate using some avgS/avgP/avgO and dummyWeight
            long estimate = switch (vars) { //noinspection DataFlowIssue
                case EMPTY        -> 1; // already handled, repeat to satisfy compiler
                case PRE_OBJ      -> spo.estimatePairs(s);
                case SUB_OBJ      -> pso.estimatePairs(p);
                case SUB_PRE      -> ops.estimatePairs(o);
                case SUB_PRE_OBJ  -> spo.triplesCount();
                case SUB -> dummies == EMPTY ? ops.estimateValues(o, p)
                                             : (int)(1 + avgS*dummyWeight);
                case OBJ -> dummies == EMPTY ? spo.estimateValues(s, p)
                                             : (int)(1 + avgO*dummyWeight);
                case PRE -> {
                    if (dummies == EMPTY)
                        dummyWeight = Math.min(dummyWeight, spo.estimatePairs(s));
                    yield (int)(1 + avgP * dummyWeight);
                }
            };
            return (int)Math.min(Integer.MAX_VALUE, estimate);
        }

        @Override protected Plan bind(Plan plan) {
            return supportedQueryMeat(plan) == null ? bindInner(plan) : new Query(plan, client);
        }
    }

    /* --- --- --- query --- --- --- */

    private static Plan supportedQueryMeat(Plan p) {
        if (p.type == MODIFIER) {
            p = p.left();
            if (p.type == UNION || p.type == JOIN) {
                var tp = p.right == null && p.left instanceof TriplePattern t ? t : null;
                if (tp != null || p.type == UNION) return tp;
            }
        }
        return switch (p.type) {
            case TRIPLE -> p;
            case UNION -> p.left instanceof TriplePattern tp && p.right == null ? tp : null;
            case JOIN -> {
                Plan r = p.right;
                if (r == null)
                    yield p.left instanceof TriplePattern tp ? tp : null;
                Plan rJoin = p;
                int rIdx = 1, n = p.opCount();
                if (n == 2) {
                    if (r.type == MODIFIER)
                        r = r.left();
                    if (r.type == JOIN) {
                        rJoin = r;
                        rIdx = 0;
                    } else {
                        yield r.type == TRIPLE ? p : null;
                    }
                }
                while (rIdx < n && rJoin.op(rIdx).type == TRIPLE) ++rIdx;
                yield rIdx == n ? p : null;
            }
            default -> null;
        };
    }

    @Override public <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql) {
        Plan plan = SparqlParser.parse(sparql);
        Plan meat = supportedQueryMeat(plan);
        BIt<StoreBatch> storeIt = null;
        var l = meat == null ? null : lookup(dictId);
        if (meat instanceof TriplePattern tp) {
            Modifier m = plan instanceof Modifier mod ? mod : null;
            // if possible, push projection to when turning Triple.* iterators into batches
            Vars tpVars = m != null && m.filters.isEmpty() ? m.publicVars() : tp.publicVars();
            storeIt = queryTP(tpVars, tp, tp.s.type() == VAR ? NOT_FOUND : l.find(tp.s),
                                          tp.p.type() == VAR ? NOT_FOUND : l.find(tp.p),
                                          tp.o.type() == VAR ? NOT_FOUND : l.find(tp.o),
                                          tp.varRoles());
            if (m != null) // apply the modifier. executeFor() detects a no-op
                storeIt = m.executeFor(storeIt, null, false);
            storeIt.metrics(Metrics.createIf(plan));
        } else if (meat != null) {
            storeIt = queryBGP(plan == sparql ? plan.deepCopy() : plan, l);
        }
        if (meat == null || storeIt == null)
            return federator.execute(batchType, plan);
        return batchType.convert(storeIt);
    }

    private BIt<StoreBatch> queryBGP(Plan joinOrMod, LocalityCompositeDict.Lookup lookup) {
        Vars outVars = joinOrMod.publicVars();
        joinOrMod = federator.shallowOptimize(joinOrMod);
        Plan join = joinOrMod.type == MODIFIER ? joinOrMod.left() : joinOrMod;
        Plan rightJoin = join;
        int rIdx = 1, n = join.opCount();
        if (n == 2) {
            Plan r = join.right();
            if (r.type == MODIFIER)
                r = (join.right = federator.shallowOptimize(r)).left();
            switch (r.type) {
                case JOIN   -> { rightJoin = r; rIdx = 0; }
                case TRIPLE ->   rIdx = 2; // accept even if wrapped in Modifier
                default     -> { return null; }
            }
        }
        if (!canExecuteRightBGP(rightJoin, rIdx))
            return null;
        Plan r = join.right();
        if (rightJoin == join && n > 2) { //noinspection DataFlowIssue
            r = n == 3 ? new Join(r, join.op(2))
                       : new Join(copyOfRange(join.operandsArray, 1, n));
        }
        var metrics = Metrics.createIf(join);
        var bq = new BindQuery<>(r, query(TYPE, join.left), BindType.JOIN,
                                 metrics == null ? null : metrics.joinMetrics[1]);
        BIt<StoreBatch> it;
        var tp = (r.type == MODIFIER ? r.left : r) instanceof TriplePattern t ? t : null;
        if (tp != null) {
            long s = NOT_FOUND, p = NOT_FOUND, o = NOT_FOUND;
            boolean empty = (tp.s.type() != VAR && (s = lookup.find(tp.s)) == NOT_FOUND)
                         || (tp.p.type() != VAR && (p = lookup.find(tp.p)) == NOT_FOUND)
                         || (tp.o.type() != VAR && (o = lookup.find(tp.o)) == NOT_FOUND);
            if (empty)
                return new EmptyBIt<>(TYPE, outVars, metrics);
            var notifier = new BindingNotifier(bq);
            it = new StoreBindingBIt<>(bq.bindings, bq.type, r, tp, s, p, o,
                                         outVars, notifier, notifier, lookup);
        } else {
            it = bindWithModifiedBGP(bq, r, outVars, lookup);
        }
        if (joinOrMod instanceof Modifier m)
            it = m.executeFor(it, null, false);
        return it;
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

    @Override public <B extends Batch<B>> BIt<B> query(BindQuery<B> bq) {
        try {
            Plan right = bq.query instanceof Plan p ? p : SparqlParser.parse(bq.query);
            if (bq.query.isGraph())
                throw new InvalidSparqlQueryType("query() method only takes SELECT/ASK queries");
            Vars outVars = bq.resultVars();
            BIt<B> it;
            var l = lookup(dictId);
            var tp = (right.type == MODIFIER ? right.left : right) instanceof TriplePattern t
                   ? t : null;
            if (tp != null) {
                long s = NOT_FOUND, p = NOT_FOUND, o = NOT_FOUND;
                boolean empty = (tp.s.type() != VAR && (s = l.find(tp.s)) == NOT_FOUND)
                             || (tp.p.type() != VAR && (p = l.find(tp.p)) == NOT_FOUND)
                             || (tp.o.type() != VAR && (o = l.find(tp.o)) == NOT_FOUND);
                if (empty) {
                    it = new EmptyBIt<>(bq.bindings.batchType(), outVars);
                } else {
                    var notifier = new BindingNotifier(bq);
                    if (right instanceof Modifier m && !m.filters.isEmpty()) {
                        var split = Optimizer.splitFilters(m.filters);
                        if (split != m.filters) {
                            var m2 = m == bq.query ? (Modifier)m.copy() : m;
                            m2.filters = split;
                            right = m2;
                        }
                    }
                    it = new StoreBindingBIt<>(bq.bindings, bq.type, right, tp, s, p, o,
                                               outVars, notifier, notifier, l);
                }
            } else {
                Plan rightJoin = right.type == MODIFIER ? right.left() : right;
                if (rightJoin.type == JOIN && canExecuteRightBGP(rightJoin, 0)) {
                    if (right == bq.query) {
                        right = right.copy();
                        if (right.type == MODIFIER)
                            right.left = rightJoin.copy();
                    }
                    right = federator.shallowOptimize(right, bq.bindings.vars());
                    it = bindWithModifiedBGP(bq, right, outVars, l);
                } else {
                    it = null;
                }
                if (it == null)
                    it = new ClientBindingBIt<>(bq, this);
            }
            return it;
        } catch (Throwable t) {
            throw FSException.wrap(endpoint, t);
        }
    }

    private static boolean canExecuteRightBGP(Plan right, int rIdx) {
        for (Plan o; rIdx < right.opCount(); ++rIdx) {
            o = right.op(rIdx);
            if (o.type != TRIPLE && (o.type != MODIFIER || o.left().type != TRIPLE)) return false;
        }
        return true;
    }

    private <B extends Batch<B>>
    BIt<B> bindWithModifiedBGP(BindQuery<B> bq, Plan modOrJoin, Vars outVars,
                               LocalityCompositeDict.Lookup lookup) {
        Modifier outerMod;
        Plan join;
        if (modOrJoin instanceof Modifier m) {
            outerMod = m;
            join = modOrJoin.left();
        } else {
            outerMod = null;
            join = modOrJoin;
        }
        int n            = join.opCount();
        var left         = bq.bindings;
        var bindNotifier = new BindingNotifier(bq);
        int weakDedupCap = outerMod != null && outerMod.distinctCapacity > 0
                                            && outerMod.offset == 0
                         ? dedupCapacity() : 0;
        for (int i = 0; i < n; i++) {
            var op = join.op(i);
            var tp = op instanceof TriplePattern t ? t : (TriplePattern)op.left();
            long s = NOT_FOUND, p = NOT_FOUND, o = NOT_FOUND;
            boolean empty = (tp.s.type() != VAR && (s = lookup.find(tp.s)) == NOT_FOUND)
                         || (tp.p.type() != VAR && (p = lookup.find(tp.p)) == NOT_FOUND)
                         || (tp.o.type() != VAR && (o = lookup.find(tp.o)) == NOT_FOUND);
            if (empty)
                return new EmptyBIt<>(left.batchType(), outVars, bq.metrics);
            Vars vars = BindType.JOIN.resultVars(left.vars(), tp.publicVars());
            BindingNotifier opBindNotifier = null;
            if (i == n -1) {
                if (outerMod != null)
                    op = modifyLastOperand(outerMod.filters, weakDedupCap, op);
                vars = outVars;
                opBindNotifier = bindNotifier;
            } else if (weakDedupCap > 0) {
                op = FS.distinct(op, weakDedupCap);
            }
            left = new StoreBindingBIt<>(left, BindType.JOIN, op, tp, s, p, o, vars,
                                         i == 0 ? bindNotifier : null, opBindNotifier, lookup);
        }
        if (outerMod != null && (outerMod.limit != Long.MAX_VALUE || outerMod.offset > 0
                || outerMod.distinctCapacity > weakDedupCap)) {
            if (!outerMod.filters.isEmpty() || outerMod.projection != null) {
                var m2 = (Modifier) outerMod.copy();
                m2.filters = List.of();
                m2.projection = null;
                outerMod = m2;
            }
            outerMod.executeFor(left, null, false);
        }
        return left;
    }

    private Plan modifyLastOperand(List<Expr> filters, int dedupCap, Plan op) {
        if (filters.isEmpty())
            return op; // no op
        if (op instanceof Modifier m) {
            if (!m.filters.isEmpty()) {
                List<Expr> outerFilters = filters;
                filters = new ArrayList<>(m.filters.size()+outerFilters.size());
                filters.addAll(m.filters);
                filters.addAll(outerFilters);
            }
            op = m.left();
        }
        return new Modifier(op, null, dedupCap, 0, Long.MAX_VALUE, filters);
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

    private static final class BindingNotifier {
        private final BindQuery<?> bindQuery;
        private long seq = -1;
        private boolean notified = false;

        private BindingNotifier(BindQuery<?> bindQuery) {
            this.bindQuery = bindQuery;
        }

        public void startBinding() {
            ++seq;
            notified = false;
            var metrics = bindQuery.metrics;
            if (metrics != null) metrics.beginBinding();
        }

        public void notifyBinding(boolean empty) {
            if (!notified) {
                notified = true;
                if (empty) bindQuery.   emptyBinding(seq);
                else       bindQuery.nonEmptyBinding(seq);
            }
        }
    }

    final class StoreBindingBIt<B extends Batch<B>> extends AbstractBIt<B> {
        private final @Nullable BindingNotifier startBindingNotifier;
        private final @Nullable BindingNotifier bindingNotifier;
        private final BIt<B> left;
        private final BindType bindType;
        private Object rIt;
        private B lb;
        private int lr;
        private B rb;
        private @Nullable B fb;
        private final @Nullable BatchMerger<B> merger;
        private final TripleRoleSet tpFreeRoles;
        private final long s, p, o;
        private long sNotLex, pNotLex, oNotLex;
        private final byte sLeftCol, pLeftCol, oLeftCol;
        private final byte kCol, skCol, vCol, tpFreeCols;
        private final short dictId, prbCols;
        private boolean rEnd, rEmpty;
        private final boolean rightSingleRow;
        private final TwoSegmentRope ropeView;
        private final @Nullable BatchMerger<B> preFilterMerger;
        private final @Nullable BatchFilter<B> rightFilter;
        private final boolean hasLexicalJoin;
        private final LocalityCompositeDict.@Nullable LocalityLexIt sLexIt, pLexIt, oLexIt;

        public StoreBindingBIt(BIt<B> left, BindType bindType, Plan right, TriplePattern tp,
                               long s, long p, long o, Vars projection,
                               @Nullable BindingNotifier startNotifier,
                               @Nullable BindingNotifier notifier,
                               LocalityCompositeDict.Lookup lookup) {
            super(left.batchType(), projection);
            this.minWaitNs = BIt.QUICK_MIN_WAIT_NS;
            this.left = left;
            this.bindType = bindType;
            this.startBindingNotifier = startNotifier;
            this.bindingNotifier = notifier;
            this.metrics = notifier == null ? null : notifier.bindQuery.metrics;
            this.ropeView = batchType == TYPE ? null : new TwoSegmentRope();
            this.dictId = (short)StoreSparqlClient.this.dictId;
            Vars leftVars = left.vars();

            // detect and setup lexical joins: FILTER(str(?right) = str(?left))
            var m = right instanceof Modifier mod ? mod : null;
            List<Expr> mFilters = m == null ? List.of() : m.filters;
            Term sTerm, pTerm, oTerm;
            if (!mFilters.isEmpty()) {
                sTerm = leftLexJoinVar(mFilters, tp.s, leftVars);
                pTerm = tp.p == tp.s ? sTerm : leftLexJoinVar(mFilters, tp.p, leftVars);
                if      (tp.o.equals(tp.s)) oTerm = sTerm;
                else if (tp.o.equals(tp.p)) oTerm = pTerm;
                else                        oTerm = leftLexJoinVar(mFilters, tp.o, leftVars);
                LocalityCompositeDict.LocalityLexIt sLexIt = null, pLexIt = null, oLexIt = null;
                if (sTerm != tp.s) {
                    if (sTerm.isVar()) { s = 0; sLexIt = dict.lexIt(); }
                    else               { s = lookup.find(sTerm); }
                }
                if (pTerm != tp.p) {
                    if (pTerm.isVar()) { p = 0; pLexIt = dict.lexIt(); }
                    else               { p = lookup.find(pTerm); }
                }
                if (oTerm != tp.o) {
                    if (oTerm.isVar()) { o = 0; oLexIt = dict.lexIt(); }
                    else               { o = lookup.find(oTerm); }
                }
                this.sLexIt = sLexIt; this.pLexIt = pLexIt; this.oLexIt = oLexIt;
                this.hasLexicalJoin = sLexIt != null || pLexIt != null || oLexIt != null;
            } else {
                sTerm  = tp.s; pTerm  = tp.p; oTerm  = tp.o;
                sLexIt = null; pLexIt = null; oLexIt = null;
                hasLexicalJoin = false;
            }

            // setup join
            this.s = s;
            this.p = p;
            this.o = o;
            int sLeftCol = leftVars.indexOf(sTerm);
            int pLeftCol = leftVars.indexOf(pTerm);
            int oLeftCol = leftVars.indexOf(oTerm);
            if ((sLeftCol|pLeftCol|oLeftCol) > 127)
                throw new IllegalArgumentException("binding column >127");
            this.sLeftCol = (byte) sLeftCol;
            this.pLeftCol = (byte) pLeftCol;
            this.oLeftCol = (byte) oLeftCol;
            Vars tpFree = new Vars.Mutable(3);
            int tpFreeRolesBits = 0;
            if (sLeftCol < 0 && tp.s.isVar()) { tpFreeRolesBits |= 4; tpFree.add(tp.s); }
            if (pLeftCol < 0 && tp.p.isVar()) { tpFreeRolesBits |= 2; tpFree.add(tp.p); }
            if (oLeftCol < 0 && tp.o.isVar()) { tpFreeRolesBits |= 1; tpFree.add(tp.o); }
            tpFreeRoles = TripleRoleSet.fromBitset(tpFreeRolesBits);
            this.tpFreeCols = (byte) tpFree.size();
            this.rb = batchType.create(BIt.PREFERRED_MIN_BATCH, tpFreeCols, 0);

            // setup index iterators for tp
            byte sCol = (byte)tpFree.indexOf(tp.s);
            byte pCol = (byte)tpFree.indexOf(tp.p);
            byte oCol = (byte)tpFree.indexOf(tp.o);
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

            //setup filtering for right-side results
            Vars procRightFree;
            if (m != null && (!mFilters.isEmpty()
                    || (m.limit > 1 && m.limit < Long.MAX_VALUE)
                    || m.offset > 0
                    || m.distinctCapacity > 0)) {
                boolean needsLeftVars = false;
                for (Expr expr : mFilters) {
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
                case JOIN, LEFT_JOIN           -> m != null && m.limit == 1;
                case EXISTS, NOT_EXISTS, MINUS -> true;
            };
            this.rEnd = true;
            this.rEmpty = true;
            this.merger = !bindType.isJoin() || procRightFree.equals(projection) ? null
                    : batchType.merger(projection, leftVars, procRightFree);
            this.lb = batchType.create(PREFERRED_MIN_BATCH, leftVars.size(),
                                       PREFERRED_MIN_BATCH*32);
            acquireRef();
        }

        /**
         * Get the var {@code v} in {@code bindings} for which there is a lexical join (i.e., a
         * {@code FILTER(str(term) = str(v))}), or return {@code term} itself if there is no such
         * {@code v}.
         *
         * @param filters set of filters, post-{@link Optimizer#splitFilters(List)} to scan for a
         *                lexical join.
         * @param term A term from the right-side operand of the join
         * @param bindings The set of vars in the left-side operand of the join.
         * @return the aforementioned {@code v}, if it exists, else, {@code term}.
         */
        private static Term leftLexJoinVar(List<Expr> filters, Term term, Vars bindings) {
            if (!term.isVar() || filters.isEmpty()) return term;
            for (var it = filters.iterator(); it.hasNext(); ) {
                Expr expr = it.next();
                if (expr instanceof Expr.Eq eq
                        && eq.l instanceof Expr.Str sl && eq.r instanceof Expr.Str sr
                        && sl.in instanceof Term l && sr.in instanceof Term r) {
                    Term selected;
                    if      (term.equals(l) && (r.isVar() || bindings.contains(r))) selected = r;
                    else if (term.equals(r) && (l.isVar() || bindings.contains(l))) selected = l;
                    else                                                            continue;

                    it.remove();
                    return selected;
                }
            }
            return term;
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            try {
                lb = batchType.recycle(lb);
                rb = batchType.recycle(rb);
                fb = batchType.recycle(fb);
                super.cleanup(cause);
            } finally { releaseRef(); }
        }

        @Override public @Nullable B nextBatch(@Nullable B b) {
            if (lb == null) return null; // already exhausted
            try {
                long startNs = needsStartTime ? Timestamp.nanoTime() : Timestamp.ORIGIN;
                long innerDeadline = rightSingleRow ? Timestamp.ORIGIN-1 : startNs+minWaitNs;
                b = getBatch(b);
                do {
                    if (rEnd && !rebind())
                        break; // reached end of bindings
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
                            for (var it = (Triples.ScanIt)rIt; it.advance(); ) {
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
                    if (prb.rows > 0)
                        rEmpty = false;
                    boolean pub = switch (bindType) {
                        case JOIN,EXISTS      -> !rEnd;
                        case NOT_EXISTS,MINUS ->  rEnd;
                        case LEFT_JOIN        -> !rEnd || rEmpty;
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
                            case EXISTS,NOT_EXISTS,MINUS -> b.putRow(lb, lr);
                        }
                    }
                    if (rightSingleRow && !rEmpty) {
                        rEnd = true;
                        if (hasLexicalJoin) endLexical();
                    }
                    if (bindingNotifier != null) bindingNotifier.notifyBinding(!pub && rEnd);
                } while (readyInNanos(b.rows, startNs) > 0);
                if (bindingNotifier != null && bindingNotifier.bindQuery.metrics != null)
                    bindingNotifier.bindQuery.metrics.batch(b.rows);
                if (b.rows == 0) b = handleEmptyBatch(b);
                else             onBatch(b);
            } catch (Throwable t) {
                onTermination(t);
                lb = null; // signal exhaustion
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
                    int slot = (int)(t.fstOff & PREFIXES_MASK);
                    var cached = prefixes[slot];
                    if (cached == null || cached.offset != t.fstOff || cached.segment != t.fst)
                        prefixes[slot] = cached = new SegmentRope(t.fst, t.fstOff, t.fstLen);
                    yield cached;
                }
                case '_' -> ByteRope.EMPTY;
                default -> throw new IllegalArgumentException("Not an RDF term");
            };
            int localOff = fst == '<' ? sh.len : 0;
            dest.putTerm(col, sh, t, localOff, t.len-sh.len, fst == '"');
        }

        private boolean resetLexIt(LexIt lexIt, byte leftCol) {
            if (!lb.getRopeView(lr, leftCol, ropeView)) return false;
            lexIt.find(ropeView);
            return lexIt.advance();
        }

        private boolean nextLexical() {
            if (oLexIt != null && oLexIt.advance())
                return true;
            if (pLexIt != null) {
                while (pLexIt.advance()) {
                    if (oLexIt == null || resetLexIt(oLexIt, oLeftCol)) return true;
                }
            }
            if (sLexIt != null) {
                while (sLexIt.advance()) {
                    if (pLexIt != null) {
                        if (!resetLexIt(pLexIt, pLeftCol)) continue; // next s
                        if (oLexIt == null) return true;
                        do {
                            if (resetLexIt(oLexIt, oLeftCol)) return true;
                        } while (pLexIt.advance());
                    } else if (resetLexIt(oLexIt, oLeftCol)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private void endLexical() {
            if (sLexIt != null) sLexIt.end();
            if (pLexIt != null) pLexIt.end();
            if (oLexIt != null) oLexIt.end();
        }

        private long lexRebindCol(LexIt lexIt, byte leftCol, long fallback,
                                  B lb, int lr, LocalityCompositeDict.Lookup l) {
            if (leftCol < 0) return fallback;
            if (lexIt == null && lb instanceof StoreBatch sb) {
                long sourced = sb.id(lr, leftCol);
                if (IdTranslator.dictId(sourced) == dictId)
                    return unsource(sourced);
            }
            if (!lb.getRopeView(lr, leftCol, ropeView))
                return fallback;
            if (lexIt == null)
                return l.find(ropeView);
            lexIt.find(ropeView);
            return lexIt.advance() ? lexIt.id : fallback;
        }

        private boolean rebindLexical() {
            long s, p, o;
            if (nextLexical()) {
                s = sLexIt == null ? sNotLex : sLexIt.id;
                p = pLexIt == null ? pNotLex : pLexIt.id;
                o = oLexIt == null ? oNotLex : oLexIt.id;
            } else {
                if (++lr >= lb.rows) {
                    lr = 0;
                    lb = left.nextBatch(lb);
                    if (startBindingNotifier != null) startBindingNotifier.startBinding();
                    if (lb                   == null) return false;
                }
                rEmpty = true;
                var l = lookup(dictId);
                sNotLex = s = lexRebindCol(sLexIt, sLeftCol, this.s, lb, lr, l);
                pNotLex = p = lexRebindCol(pLexIt, pLeftCol, this.p, lb, lr, l);
                oNotLex = o = lexRebindCol(oLexIt, oLeftCol, this.o, lb, lr, l);
            }
            rebind(s, p, o);
            return true;
        }

        private boolean rebind() {
            if (hasLexicalJoin)
                return rebindLexical();
            if (++lr >= lb.rows) {
                lr = 0;
                lb = left.nextBatch(lb);
                if (lb                   == null) return false;
                if (startBindingNotifier != null) startBindingNotifier.startBinding();
            }
            rEmpty = true;
            B lb = this.lb;
            int lr = this.lr;
            long s, p, o;
            var l = lookup(dictId);
            if (lb instanceof StoreBatch sb) {
                s = sLeftCol < 0 ? this.s : translate(sb.id(lr, sLeftCol), dictId, l);
                p = pLeftCol < 0 ? this.p : translate(sb.id(lr, pLeftCol), dictId, l);
                o = oLeftCol < 0 ? this.o : translate(sb.id(lr, oLeftCol), dictId, l);
            } else {
                s = sLeftCol >= 0 && lb.getRopeView(lr, sLeftCol, ropeView)
                        ? l.find(ropeView) : this.s;
                p = pLeftCol >= 0 && lb.getRopeView(lr, pLeftCol, ropeView)
                        ? l.find(ropeView) : this.p;
                o = oLeftCol >= 0 && lb.getRopeView(lr, oLeftCol, ropeView)
                        ? l.find(ropeView) : this.o;
            }
            rebind(s, p, o);
            return true;
        }

        private void rebind(long s, long p, long o) {
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
            if (metrics instanceof Metrics.JoinMetrics jm) jm.beginBinding();
        }
    }
}
