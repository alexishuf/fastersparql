package com.github.alexishuf.fastersparql.store;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.*;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.base.UnitaryBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchFilter;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.EmitBindQuery;
import com.github.alexishuf.fastersparql.client.ItBindQuery;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.async.TaskEmitter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.stages.BindingStage;
import com.github.alexishuf.fastersparql.emit.stages.ConverterStage;
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
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
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
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.github.alexishuf.fastersparql.FSProperties.dedupCapacity;
import static com.github.alexishuf.fastersparql.batch.type.Batch.asUnpooled;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.model.TripleRoleSet.EMPTY;
import static com.github.alexishuf.fastersparql.model.TripleRoleSet.*;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.operators.plan.Operator.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.GROUND;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Type.VAR;
import static com.github.alexishuf.fastersparql.store.batch.IdTranslator.*;
import static com.github.alexishuf.fastersparql.store.index.dict.Dict.NOT_FOUND;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.copyOfRange;
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;

public class StoreSparqlClient extends AbstractSparqlClient
                               implements CardinalityEstimatorProvider {
    private static final Logger log = LoggerFactory.getLogger(StoreSparqlClient.class);
    private static final StoreBatchType TYPE = StoreBatchType.INSTANCE;
    private static final int PREFIXES_MASK = -1 >>> Integer.numberOfLeadingZeros(
            (8*1024*1024)/(4/* SegmentRope ref */ + 32/* SegmentRope obj */));
    private static final LIFOPool<SegmentRope[]> PREFIXES_POOL
            = new LIFOPool<>(SegmentRope[].class, 16);

    private final LocalityCompositeDict dict;
    private final int dictId;
    private final Triples spo, pso, ops;
    private final SingletonFederator federator;
    private final SegmentRope[] prefixes;

    /* --- --- --- lifecycle --- --- --- */

    public StoreSparqlClient(SparqlEndpoint ep) {
        super(ep);
        SegmentRope[] prefixes = PREFIXES_POOL.get();
        this.prefixes = prefixes == null ? new SegmentRope[PREFIXES_MASK+1] : prefixes;
        this.bindingAwareProtocol = true;
        Path dir = endpoint.asFile().toPath();
        boolean validate = FSProperties.storeClientValidate();
        log.debug("Loading{} {}...", validate ? "/validating" : "", dir);
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
        log.debug("Loaded{} {}...", validate ? "/validated" : "", dir);
    }

    private static Triples loadTriples(Path path, boolean validate) throws IOException {
        Triples t = new Triples(path);
        if (validate)
            t.validate();
        return t;
    }

    public class Guard extends RefGuard {
        public Guard() { acquireRef(); }
        public StoreSparqlClient get() { return StoreSparqlClient.this; }
    }

    public Guard retain() { return new Guard(); }

    @Override protected void doClose() {
        log.debug("Closing {}", this);
        IdTranslator.deregister(dictId, dict);
        dict.close();
        spo.close();
        pso.close();
        ops.close();
        PREFIXES_POOL.offer(prefixes);
    }

    /* --- --- --- properties--- --- --- */

    void usesBindingAwareProtocol(boolean value) { bindingAwareProtocol = value; }
    public int dictId() { return dictId; }

    /* --- --- --- estimation & optimization --- --- --- */

    public CardinalityEstimator estimator() { return federator; }

    public int estimate(TriplePattern tp) { return federator.estimate(tp, null); }

    private final class StoreSingletonFederator extends SingletonFederator {
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
            var vars = binding == null ? t.freeRoles() : t.freeRoles(binding);
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

        @Override
        protected <B extends Batch<B>> Emitter<B> convert(BatchType<B> dest, Emitter<?> in) {
            //noinspection unchecked
            return new FromStoreConverter<>(dest, (Emitter<StoreBatch>)in);
        }

        @Override protected Plan bind2client(Plan plan, QueryMode mode) {
            boolean supported = switch (mode) {
                case ITERATOR -> supportedQueryMeat(plan) != null;
                case EMIT -> (plan.type == MODIFIER ? plan.left() : plan).type == TRIPLE;
            };
            return supported ? new Query(plan, client) : bindInner(plan, mode);
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

    @Override public <B extends Batch<B>> BIt<B> doQuery(BatchType<B> batchType, SparqlQuery sparql) {
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
                    tp.freeRoles());
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
                case JOIN   -> {
                    if (join.type != JOIN)
                        return null; // can't handle LeftJoin|Exists|Minus(L, Join(...))
                    rightJoin = r; rIdx = 0;
                }
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
        var bq = new ItBindQuery<>(r, query(TYPE, join.left()), join.type.bindType(),
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

//    private Emitter<StoreBatch> emitBGP(Plan joinOrMod) {
//        Vars outVars = joinOrMod.publicVars();
//        joinOrMod = federator.shallowOptimize(joinOrMod);
//        Plan join = joinOrMod.type == MODIFIER ? joinOrMod.left() : joinOrMod;
//        Plan rightJoin = join;
//        int rIdx = 1, n = join.opCount();
//        if (n == 2) {
//            Plan r = join.right();
//            if (r.type == MODIFIER)
//                r = (join.right = federator.shallowOptimize(r)).left();
//            switch (r.type) {
//                case JOIN   -> {
//                    if (join.type != JOIN)
//                        return null; // can't handle LeftJoin|Exists|Minus(L, Join(...))
//                    rightJoin = r; rIdx = 0;
//                }
//                case TRIPLE ->   rIdx = 2; // accept even if wrapped in Modifier
//                default     -> { return null; }
//            }
//        }
//        if (!canExecuteRightBGP(rightJoin, rIdx))
//            return null;
//        Plan r = join.right();
//        if (rightJoin == join && n > 2) { //noinspection DataFlowIssue
//            r = n == 3 ? new Join(r, join.op(2))
//                    : new Join(copyOfRange(join.operandsArray, 1, n));
//        }
//        var bq = new EmitBindQuery<>(r, doEmit(TYPE, join.left()), join.type.bindType());
//        Emitter<StoreBatch> em;
//        var tp = (r.type == MODIFIER ? r.left : r) instanceof TriplePattern t ? t : null;
//        var lookup = dict.lookup();
//        if (tp != null) {
//            long s = NOT_FOUND, p = NOT_FOUND, o = NOT_FOUND;
//            boolean empty = (tp.s.type() != VAR && (s = lookup.find(tp.s)) == NOT_FOUND)
//                         || (tp.p.type() != VAR && (p = lookup.find(tp.p)) == NOT_FOUND)
//                         || (tp.o.type() != VAR && (o = lookup.find(tp.o)) == NOT_FOUND);
//            if (empty)
//                return Emitters.empty(TYPE, outVars);
//            em = new StoreBindingEmitter<>(bq.bindings, bq.type, r, tp, s, p, o,
//                                           outVars, lookup, bq);
//        } else {
//            em = bindWithModifiedBGP(bq, r, outVars, lookup);
//        }
//        if (joinOrMod instanceof Modifier m)
//            em = m.processed(em);
//        return em;
//    }

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

    @Override public <B extends Batch<B>> Emitter<B>
    doEmit(BatchType<B> bt, SparqlQuery sparql, Vars rebindHint) {
        Plan plan = SparqlParser.parse(sparql);
        var tp = (plan.type == MODIFIER ? plan.left : plan) instanceof TriplePattern t ? t : null;
        Emitter<StoreBatch> storeEm;
        if (tp != null) {
            Modifier m = plan instanceof Modifier mod ? mod : null;
            Vars tpVars = (m != null && m.filters.isEmpty() ? m : tp).publicVars();
            storeEm = new TPEmitter(tp, tpVars);
            if (m != null) // apply any modification required (projection may be done already)
                storeEm = m.processed(storeEm);
        } else {
            storeEm = federator.emit(TYPE, plan, rebindHint);
        }
        //noinspection unchecked
        return bt == TYPE ? (Emitter<B>) storeEm : new FromStoreConverter<>(bt, storeEm);
    }

    @Override public <B extends Batch<B>> BIt<B> doQuery(ItBindQuery<B> bq) {
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
            if (bq.type == BindType.JOIN && rightJoin.type == JOIN
                                         && canExecuteRightBGP(rightJoin, 0)) {
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
    }

    @Override public <B extends Batch<B>> Emitter<B> doEmit(EmitBindQuery<B> bq,
                                                            Vars rebindHint) {
        Plan right = bq.query instanceof Plan p ? p : SparqlParser.parse(bq.query);
        if (right instanceof Modifier m && !m.filters.isEmpty())
            right = splitFiltersForLexicalJoin(m, m == bq.query);
        return new StoreBindingEmitter<>(bq, right, rebindHint);
    }

    private Modifier splitFiltersForLexicalJoin(Modifier m, boolean copyOnChange) {
        var split = Optimizer.splitFilters(m.filters);
        if (split != m.filters) {
            var m2 = copyOnChange ? (Modifier)m.copy() : m;
            m2.filters = split;
            return m2;
        }
        return m;
    }

    private static boolean canExecuteRightBGP(Plan right, int rIdx) {
        for (Plan o; rIdx < right.opCount(); ++rIdx) {
            o = right.op(rIdx);
            if (o.type != TRIPLE && (o.type != MODIFIER || o.left().type != TRIPLE)) return false;
        }
        return true;
    }

    private <B extends Batch<B>>
    BIt<B> bindWithModifiedBGP(ItBindQuery<B> bq, Plan modOrJoin, Vars outVars,
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

//    private <B extends Batch<B>>
//    Emitter<B> bindWithModifiedBGP(EmitBindQuery<B> bq, Plan modOrJoin, Vars outVars,
//                                   LocalityCompositeDict.Lookup lookup) {
//        Modifier outerMod;
//        Plan join;
//        if (modOrJoin instanceof Modifier m) {
//            outerMod = m;
//            join = modOrJoin.left();
//        } else {
//            outerMod = null;
//            join = modOrJoin;
//        }
//        int n            = join.opCount();
//        var left         = bq.bindings;
//        int weakDedupCap = outerMod != null && outerMod.distinctCapacity > 0
//                && outerMod.offset == 0
//                ? dedupCapacity() : 0;
//        for (int i = 0; i < n; i++) {
//            var op = join.op(i);
//            var tp = op instanceof TriplePattern t ? t : (TriplePattern)op.left();
//            long s = NOT_FOUND, p = NOT_FOUND, o = NOT_FOUND;
//            boolean empty = (tp.s.type() != VAR && (s = lookup.find(tp.s)) == NOT_FOUND)
//                    || (tp.p.type() != VAR && (p = lookup.find(tp.p)) == NOT_FOUND)
//                    || (tp.o.type() != VAR && (o = lookup.find(tp.o)) == NOT_FOUND);
//            if (empty)
//                return Emitters.empty(left.batchType(), outVars);
//            Vars vars = BindType.JOIN.resultVars(left.vars(), tp.publicVars());
//            EmitBindQuery<B> opBindListener = null;
//            if (i == n -1) {
//                if (outerMod != null)
//                    op = modifyLastOperand(outerMod.filters, weakDedupCap, op);
//                vars = outVars;
//                opBindListener = bq;
//            } else if (weakDedupCap > 0) {
//                op = FS.distinct(op, weakDedupCap);
//            }
//            left = new StoreBindingEmitter<>(left, BindType.JOIN, op, tp, s, p, o, vars,
//                                             lookup, opBindListener);
//        }
//        if (outerMod != null && (outerMod.limit != Long.MAX_VALUE || outerMod.offset > 0
//                || outerMod.distinctCapacity > weakDedupCap)) {
//            if (!outerMod.filters.isEmpty() || outerMod.projection != null) {
//                var m2 = (Modifier) outerMod.copy();
//                m2.filters = List.of();
//                m2.projection = null;
//                outerMod = m2;
//            }
//            outerMod.processed(left);
//        }
//        return left;
//    }

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

    /* --- --- --- emitters --- --- --- */

    private final class TPEmitter extends TaskEmitter<StoreBatch> {
        private static final int LIMIT_TICKS = 2;
        private static final int DEADLINE_CHK = 0xf;
        private static final int MAX_BATCH = BIt.DEF_MAX_BATCH;
        private static final int MIN_ROWS = 64;
        private static final int HAS_UNSET_OUT    = 0x01000000;
        private static final Flags FLAGS = TASK_EMITTER_FLAGS.toBuilder()
                .flag(HAS_UNSET_OUT, "HAS_UNSET_OUT")
                .build();

        private @Nullable StoreBatch recycled;
        private final byte cols;
        private byte outCol0, outCol1, outCol2;
        private final short dictId = (short)StoreSparqlClient.this.dictId;
        private byte freeRoles;
        private Object it;
        private final long[] rowSkel;
        // ---------- fields below this line are accessed only on construction/rebind()
        private byte sInCol = -1, pInCol = -1, oInCol = -1;
        private final TriplePattern tp;
        private final LocalityCompositeDict.Lookup lookup = dict.lookup();
        private @MonotonicNonNull Vars lastBindingsVars;
        private final long sId, pId, oId;
        private int @Nullable[] skelCol2InCol;
        private final TwoSegmentRope view = TwoSegmentRope.pooled();

        public TPEmitter(TriplePattern tp, Vars outVars) {
            super(TYPE, outVars, EMITTER_SVC, RR_WORKER, CREATED, FLAGS);
            int cols = outVars.size();
            if (cols > 127)
                throw new IllegalArgumentException("Too many output columns");
            this.cols = (byte) cols;
            this.tp = tp;
            Arrays.fill(rowSkel = ArrayPool.longsAtLeast(cols), 0L);
            sId = lookup.find(tp.s);
            pId = lookup.find(tp.p);
            oId = lookup.find(tp.o);
            rebind(BatchBinding.ofEmpty(TYPE));
            if (stats != null)
                stats.rebinds = 0;
            if (ResultJournal.ENABLED)
                ResultJournal.initEmitter(this, vars);
            acquireRef();
        }

        @Override protected void doRelease() {
            try {
                recycled = Batch.recyclePooled(recycled);
                ArrayPool.LONG.offer(rowSkel, rowSkel.length);
                if (skelCol2InCol != null)
                    skelCol2InCol = ArrayPool.INT.offer(skelCol2InCol, skelCol2InCol.length);
                view.recycle();
                releaseRef();
            } finally {
                super.doRelease();
            }
        }

        private int bindingVarsChanged(int state, Vars bindingVars) {
            lastBindingsVars = bindingVars;
            freeRoles = 0;
            int sInCol = -1, pInCol = -1, oInCol = -1;
            if (tp.s.isVar() && (sInCol = bindingVars.indexOf(tp.s)) < 0) freeRoles |= SUB_BITS;
            if (tp.p.isVar() && (pInCol = bindingVars.indexOf(tp.p)) < 0) freeRoles |= PRE_BITS;
            if (tp.o.isVar() && (oInCol = bindingVars.indexOf(tp.o)) < 0) freeRoles |= OBJ_BITS;
            if (sInCol > 0x7f || pInCol > 0x7f || oInCol > 0x7f)
                throw new IllegalArgumentException("Too many input columns");
            this.sInCol = (byte)sInCol;
            this.pInCol = (byte)pInCol;
            this.oInCol = (byte)oInCol;
            byte sc = (byte)vars.indexOf(tp.s);
            byte pc = (byte)vars.indexOf(tp.p);
            byte oc = (byte)vars.indexOf(tp.o);
            byte o0 = -1, o1 = -1, o2 = -1;
            switch (freeRoles) {
                case       EMPTY_BITS ->   it = FALSE;
                case         OBJ_BITS -> { it = spo.makeValuesIt(); o0 = oc; }
                case         PRE_BITS -> { it = spo.makeSubKeyIt(); o0 = pc; }
                case     PRE_OBJ_BITS -> { it = spo.makePairIt();   o0 = pc; o1 = oc; }
                case         SUB_BITS -> { it = ops.makeValuesIt(); o0 = sc; }
                case     SUB_OBJ_BITS -> { it = pso.makePairIt();   o0 = sc; o1 = oc; }
                case     SUB_PRE_BITS -> { it = ops.makePairIt();   o0 = pc; o1 = sc; }
                case SUB_PRE_OBJ_BITS -> { it = spo.scan();         o0 = sc; o1 = pc; o2 = oc; }
            }
            outCol0 = o0;
            outCol1 = o1;
            outCol2 = o2;
            int colsSet = 0;
            if (o0 != -1                        ) ++colsSet;
            if (o1 != -1 && o1 != o0            ) ++colsSet;
            if (o2 != -1 && o2 != o0 && o2 != o1) ++colsSet;
            if (colsSet < cols) {
                if (skelCol2InCol == null)
                    skelCol2InCol = ArrayPool.intsAtLeast(cols);
                for (int c = 0; c < cols; c++)
                    skelCol2InCol[c] = bindingVars.indexOf(vars.get(c));
                return setFlagsRelease(state, HAS_UNSET_OUT);
            }
            return clearFlagsRelease(state, HAS_UNSET_OUT);
        }

        @SuppressWarnings("DataFlowIssue")
        private long toUnsourcedId(int col, Batch<?> batch, int row, BatchBinding binding) {
            int cols = batch.cols;
            while (col >= cols) {
                col -= cols;
                row  = (binding = binding.remainder).row;
                cols = (batch   = binding.batch    ).cols;
            }
            if (batch instanceof StoreBatch sb)
                return translate(sb.arr[cols*row+col], dictId, lookup);
            else if (batch.getRopeView(row, col, view))
                 return lookup.find(view);
            return NOT_FOUND;
        }


        @Override public void rebind(BatchBinding binding) throws RebindException {
            Vars bVars = binding.vars;
            int st = resetForRebind(0, LOCKED_MASK);
            try {
                if (EmitterStats.ENABLED && stats != null)
                    stats.onRebind(binding);
                if (ResultJournal.ENABLED)
                    ResultJournal.rebindEmitter(this, binding);
                if (!bVars.equals(lastBindingsVars))
                    st = bindingVarsChanged(st, bVars);
                Batch<?> bb = binding.batch;
                int bRow = binding.row;
                if (bb == null || bRow >= bb.rows)
                    throw new IllegalArgumentException("invalid binding");
                long s = sInCol >= 0 ? toUnsourcedId(sInCol, bb, bRow, binding) : sId;
                long p = pInCol >= 0 ? toUnsourcedId(pInCol, bb, bRow, binding) : pId;
                long o = oInCol >= 0 ? toUnsourcedId(oInCol, bb, bRow, binding) : oId;
                if ((st&HAS_UNSET_OUT) != 0) {
                    int[] skelCol2InCol = this.skelCol2InCol;
                    if (skelCol2InCol != null) {
                        for (int c = 0; c < cols; c++) {
                            int bc = skelCol2InCol[c];
                            rowSkel[c] = bc < 0 ? NOT_FOUND
                                    : source(toUnsourcedId(bc, bb, bRow, binding), dictId);
                        }
                    }
                }
                switch (freeRoles) {
                    case       EMPTY_BITS -> it = spo.contains(s, p, o);
                    case         OBJ_BITS -> spo.values (s, p, (Triples.ValueIt )it);
                    case         PRE_BITS -> spo.subKeys(s, o, (Triples.SubKeyIt)it);
                    case     PRE_OBJ_BITS -> spo.pairs  (s,    (Triples.PairIt  )it);
                    case         SUB_BITS -> ops.values (o, p, (Triples.ValueIt )it);
                    case     SUB_OBJ_BITS -> pso.pairs  (p,    (Triples.PairIt  )it);
                    case     SUB_PRE_BITS -> ops.pairs  (o,    (Triples.PairIt  )it);
                    case SUB_PRE_OBJ_BITS -> spo.scan   (      (Triples.ScanIt  )it);
                }
            } finally {
                unlock(st);
            }
        }

        @Override public String toString() { return super.toString()+'('+tp+')'; }

        @Override protected void appendToSimpleLabel(StringBuilder out) {
            String uri = endpoint.uri();
            int begin = uri.lastIndexOf('/');
            String file = begin < 0 ? uri : uri.substring(begin);
            out.append('\n').append(tp).append(file);
        }

        @Override protected int produceAndDeliver(int state) {
            long limit = (long)REQUESTED.getOpaque(this);
            if (limit <= 0)
                return state;
            int termState = state;
            StoreBatch b = TYPE.empty(asUnpooled(recycled), MIN_ROWS, cols, 0);
            recycled = null;
            int iLimit = (int)Math.min(MAX_BATCH, limit);
            boolean retry = switch (freeRoles) {
                case                             EMPTY_BITS ->    fillAsk(b);
                case                      OBJ_BITS,SUB_BITS ->  fillValue(b, iLimit);
                case                               PRE_BITS -> fillSubKey(b, iLimit);
                case PRE_OBJ_BITS,SUB_OBJ_BITS,SUB_PRE_BITS ->   fillPair(b, iLimit);
                case                       SUB_PRE_OBJ_BITS ->   fillScan(b, iLimit);
                default                                     -> true; // unreachable
            };
            if (!retry)
                termState = COMPLETED;
            int rows = b.rows;
            if (rows > 0) {
                if ((long)REQUESTED.getAndAddRelease(this, -rows) > rows)
                    termState |= MUST_AWAKE;
                journal("produced rows=", rows, "upd req=", (long)REQUESTED.getOpaque(this));
                b = deliver(b);
            }
            recycled = Batch.asPooled(b);
            return termState;
        }

        @SuppressWarnings("SameReturnValue") private boolean fillAsk(StoreBatch b) {
            if (it == TRUE) {
                b.reserve(1, 0);
                ++b.rows;
                if ((statePlain()&HAS_UNSET_OUT) != 0)
                    fillSkel(b.arr, b.rows*cols);
                b.dropCachedHashes();
            }
            return false;
        }

        private boolean fillValue(StoreBatch b, int limit) {
            int rows = b.rows;
            long deadline = Timestamp.nextTick(LIMIT_TICKS);
            boolean retry = true, hasUnset = (statePlain()&HAS_UNSET_OUT) != 0;
            byte outCol0 = this.outCol0;
            var it = (Triples.ValueIt)this.it;
            for (int base = rows*cols; rows < limit && (retry = it.advance()); base += cols) {
                b.reserve(rows+1, 0);
                long[] a = b.arr;
                if (hasUnset) fillSkel(a, base);
                if (outCol0 >= 0) a[cols*rows + outCol0] = source(it.valueId, dictId);
                ++rows;
                if ((rows&DEADLINE_CHK) == DEADLINE_CHK && Timestamp.nanoTime() > deadline) break;
            }
            b.rows = rows;
            b.dropCachedHashes();
            return retry;
        }

        private boolean fillPair(StoreBatch b, int limit) {
            int rows = b.rows;
            long deadline = Timestamp.nextTick(LIMIT_TICKS);
            boolean retry = true, hasUnset = (statePlain()&HAS_UNSET_OUT) != 0;
            byte outCol0 = this.outCol0, outCol1 = this.outCol1;
            var it = (Triples.PairIt) this.it;
            for (int base = rows*cols; rows < limit && (retry = it.advance()); base += cols) {
                b.reserve(rows+1, 0);
                long[] a = b.arr;
                if (hasUnset) fillSkel(a, base);
                if (outCol0 >= 0) a[base + outCol0] = source(it.subKeyId, dictId);
                if (outCol1 >= 0) a[base + outCol1] = source(it.valueId,  dictId);
                ++rows;
                if ((rows&DEADLINE_CHK) == DEADLINE_CHK && Timestamp.nanoTime() > deadline) break;
            }
            b.rows = rows;
            b.dropCachedHashes();
            return retry;
        }

        private boolean fillSubKey(StoreBatch b, int limit) {
            int rows = b.rows;
            long deadline = Timestamp.nextTick(LIMIT_TICKS);
            boolean retry = true, hasUnset = (statePlain()&HAS_UNSET_OUT) != 0;
            byte outCol0 = this.outCol0;
            var it = (Triples.SubKeyIt)this.it;
            for (int base = rows*cols; rows < limit && (retry = it.advance()); base += cols) {
                b.reserve(rows+1, 0);
                long[] a = b.arr;
                if (hasUnset) fillSkel(a, base);
                if (outCol0 >= 0) a[base + outCol0] = source(it.subKeyId, dictId);
                ++rows;
                if ((rows&DEADLINE_CHK) == DEADLINE_CHK && Timestamp.nanoTime() > deadline) break;
            }
            b.rows = rows;
            b.dropCachedHashes();
            return retry;
        }

        private boolean fillScan(StoreBatch b, int limit) {
            int rows = b.rows;
            long deadline = Timestamp.nextTick(LIMIT_TICKS);
            boolean retry = false, hasUnset = (statePlain()&HAS_UNSET_OUT) != 0;
            byte outCol0 = this.outCol0, outCol1 = this.outCol1, outCol2 = this.outCol2;
            var it = (Triples.ScanIt) this.it;
            for (int base = rows*cols; rows < limit && (retry = it.advance()); base += cols) {
                b.reserve(rows+1, 0);
                long[] a = b.arr;
                if (hasUnset) fillSkel(a, base);
                if (outCol0 >= 0) a[base + outCol0] = source(it.keyId,    dictId);
                if (outCol1 >= 0) a[base + outCol1] = source(it.subKeyId, dictId);
                if (outCol2 >= 0) a[base + outCol2] = source(it.valueId,  dictId);
                ++rows;
                if ((rows&DEADLINE_CHK) == DEADLINE_CHK && Timestamp.nanoTime() > deadline) break;
            }
            b.rows = rows;
            b.dropCachedHashes();
            return retry;
        }

        private void fillSkel(long[] dest, int base) {
            //noinspection ManualArrayCopy
            for (int i = 0; i < cols; i++)
                dest[base+i] = rowSkel[i];
        }
    }

    private final class FromStoreConverter<B extends Batch<B>> extends ConverterStage<StoreBatch, B> {
        private final LocalityCompositeDict.Lookup lookup;

        public FromStoreConverter(BatchType<B> type, Emitter<StoreBatch> upstream) {
            super(type, upstream);
            this.lookup = dict.lookup();
        }

        @Override protected B putConverting(B dst, StoreBatch src) {
            int cols = src.cols, rows = src.rows;
            if (dst.cols != cols) throw new IllegalArgumentException("cols mismatch");
            long[] ids = src.arr;
            dst.reserve(rows, 8*rows);
            for (int base = 0, end = rows*cols; base < end; base += cols)
                putRowConverting(dst, ids, cols, base);
            return dst;
        }

        @Override protected void putRowConverting(B dest, StoreBatch input, int row) {
            int cols = input.cols;
            putRowConverting(dest, input.arr, cols, cols*row);
        }

        private void putRowConverting(B dest, long[] ids, int cols, int base) {
            dest.beginPut();
            for (int c = 0; c < cols; c++) {
                TwoSegmentRope t = lookup.get(unsource(ids[base + c]));
                if (t == null) continue;
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
                dest.putTerm(c, sh, t, localOff, t.len-sh.len, fst == '"');
            }
            dest.commitPut();
        }
    }

//    final class StoreBindingEmitter<B extends Batch<B>> extends BindingStage<B> {
//        private final StoreEmitter tpEmitter;
//        private final Emitter<B> convTpEmitter;
//        private final @Nullable Modifier mustRebindModifier;
//        private final Plan right;
//        private Emitter<B> rightEmitter;
//        private final LocalityCompositeDict.Lookup lookup;
//        private final short dictId;
//        private final long s, p, o;
//        private final byte sLeftCol, pLeftCol, oLeftCol;
//        private final TripleRoleSet tpFreeRoles;
//        private long sNotLex, pNotLex, oNotLex;
//        private final LocalityCompositeDict.@Nullable LocalityLexIt sLexIt, pLexIt, oLexIt;
//        private final boolean hasLexicalJoin;
//        private @MonotonicNonNull B lb;
//        private int lr;
//        private final TwoSegmentRope view = new TwoSegmentRope();
//
//        public StoreBindingEmitter(Emitter<B> left, BindType type, Plan right, TriplePattern tp,
//                                   long s, long p, long o, Vars outVars,
//                                   LocalityCompositeDict.Lookup lookup,
//                                   @Nullable BindListener listener) {
//            super(left, type, listener, outVars, right.publicVars().minus(left.vars()));
//            this.dictId = (short)StoreSparqlClient.this.dictId;
//            this.lookup = lookup;
//            this.right = right;
//            Vars leftVars = left.vars();
//
//            // detect and setup lexical joins: FILTER(str(?right) = str(?left))
//            var m = right instanceof Modifier mod ? mod : null;
//            List<Expr> mFilters = m == null ? List.of() : m.filters;
//            Term sTerm, pTerm, oTerm;
//            if (!mFilters.isEmpty()) {
//                sTerm = leftLexJoinVar(mFilters, tp.s, leftVars);
//                pTerm = tp.p == tp.s ? sTerm : leftLexJoinVar(mFilters, tp.p, leftVars);
//                if (tp.o.equals(tp.s)) oTerm = sTerm;
//                else if (tp.o.equals(tp.p)) oTerm = pTerm;
//                else oTerm = leftLexJoinVar(mFilters, tp.o, leftVars);
//                LocalityCompositeDict.LocalityLexIt sLexIt = null, pLexIt = null, oLexIt = null;
//                if (sTerm != tp.s) {
//                    if (sTerm.isVar()) {
//                        s = 0;
//                        sLexIt = dict.lexIt();
//                    } else {
//                        s = lookup.find(sTerm);
//                    }
//                }
//                if (pTerm != tp.p) {
//                    if (pTerm.isVar()) {
//                        p = 0;
//                        pLexIt = dict.lexIt();
//                    } else {
//                        p = lookup.find(pTerm);
//                    }
//                }
//                if (oTerm != tp.o) {
//                    if (oTerm.isVar()) {
//                        o = 0;
//                        oLexIt = dict.lexIt();
//                    } else {
//                        o = lookup.find(oTerm);
//                    }
//                }
//                this.sLexIt = sLexIt;
//                this.pLexIt = pLexIt;
//                this.oLexIt = oLexIt;
//                this.hasLexicalJoin = sLexIt != null || pLexIt != null || oLexIt != null;
//            } else {
//                sTerm = tp.s;
//                pTerm = tp.p;
//                oTerm = tp.o;
//                sLexIt = null;
//                pLexIt = null;
//                oLexIt = null;
//                hasLexicalJoin = false;
//            }
//
//            // setup join
//            this.s = this.sNotLex = s;
//            this.p = this.pNotLex = p;
//            this.o = this.oNotLex = o;
//            int sLeftCol = leftVars.indexOf(sTerm);
//            int pLeftCol = leftVars.indexOf(pTerm);
//            int oLeftCol = leftVars.indexOf(oTerm);
//            if ((sLeftCol | pLeftCol | oLeftCol) > 127)
//                throw new IllegalArgumentException("binding column >127");
//            this.sLeftCol = (byte) sLeftCol;
//            this.pLeftCol = (byte) pLeftCol;
//            this.oLeftCol = (byte) oLeftCol;
//
//            // setup tpEmitter
//            Vars tUseful = new Vars.Mutable(3);
//            TripleRoleSet tFreeRoles = EMPTY;
//            if (sLeftCol < 0 && tp.s.isVar()) {
//                tFreeRoles = tFreeRoles.withSub();
//                addIfUseful(tUseful, tp.s, mFilters);
//            }
//            if (pLeftCol < 0 && tp.p.isVar()) {
//                tFreeRoles = tFreeRoles.withPre();
//                addIfUseful(tUseful, tp.p, mFilters);
//            }
//            if (oLeftCol < 0 && tp.o.isVar()) {
//                tFreeRoles = tFreeRoles.withObj();
//                addIfUseful(tUseful, tp.o, mFilters);
//            }
//
//            tpEmitter = switch (this.tpFreeRoles = tFreeRoles) {
//                case EMPTY       -> new StoreAskEmitter   (tUseful, true);
//                case SUB         -> new StoreValueEmitter (tUseful, sTerm,        ops.makeValuesIt());
//                case OBJ         -> new StoreValueEmitter (tUseful, oTerm,        spo.makeValuesIt());
//                case PRE         -> new StoreSubKeyEmitter(tUseful, pTerm,        spo.makeSubKeyIt());
//                case PRE_OBJ     -> new StorePairEmitter  (tUseful, pTerm, oTerm, spo.makePairIt());
//                case SUB_OBJ     -> new StorePairEmitter  (tUseful, sTerm, oTerm, pso.makePairIt());
//                case SUB_PRE     -> new StorePairEmitter  (tUseful, pTerm, sTerm, ops.makePairIt());
//                case SUB_PRE_OBJ -> new StoreScanEmitter  (tUseful, sTerm, pTerm, oTerm);
//            };
//
//            // create emitter for modified tp
//            boolean atMostProjecting = m == null
//                    || (mFilters.isEmpty()        && m.distinctCapacity == 0 &&
//                        m.limit == Long.MAX_VALUE && m.offset == 0);
//            //noinspection unchecked
//            convTpEmitter = batchType == TYPE
//                    ? (Emitter<B>)tpEmitter : new StoreConverterStage<>(batchType, tpEmitter);
//            rightEmitter = atMostProjecting ? convTpEmitter : m.processed(convTpEmitter);
//            boolean mustRebindFilters = false;
//            for (int i = 0, n = mFilters.size(); i < n && !mustRebindFilters; i++)
//                mustRebindFilters = mFilters.get(i).vars().intersects(leftVars);
//            this.mustRebindModifier = mustRebindFilters ? m : null;
//            acquireRef();
//        }
//
//        private void addIfUseful(Vars dest, Term var, List<Expr> filters) {
//            boolean is = expectedRightVars.contains(var);
//            if (!is && !filters.isEmpty()) {
//                for (Expr e : filters) {
//                    is = e.vars().contains(var);
//                    if (is) break;
//                }
//            }
//            if (is)
//                dest.add(var);
//        }
//
//        @Override protected Object rightDisplay() {
//            return sparqlDisplay(right.sparql().toString());
//        }
//
//        @Override protected void cleanup() { releaseRef(); }
//
//        @Override protected Emitter<B> bind(BatchBinding binding) {
//            B lb = this.lb = binding.batch;
//            int lr = this.lr = binding.row;
//            assert lb != null;
//            long s = this.s, p = this.p, o = this.o;
//            if (hasLexicalJoin) {
//                sNotLex = s = lexRebindCol(sLexIt, sLeftCol, s, lb, lr);
//                pNotLex = p = lexRebindCol(pLexIt, pLeftCol, p, lb, lr);
//                oNotLex = o = lexRebindCol(oLexIt, oLeftCol, o, lb, lr);
//            } else if (lb instanceof StoreBatch sb) {
//                s = sLeftCol < 0 ? s : translate(sb.id(lr, sLeftCol), dictId, lookup);
//                p = pLeftCol < 0 ? p : translate(sb.id(lr, pLeftCol), dictId, lookup);
//                o = oLeftCol < 0 ? o : translate(sb.id(lr, oLeftCol), dictId, lookup);
//            } else {
//                s = sLeftCol >= 0 && lb.getRopeView(lr, sLeftCol, view) ? lookup.find(view) : s;
//                p = pLeftCol >= 0 && lb.getRopeView(lr, pLeftCol, view) ? lookup.find(view) : p;
//                o = oLeftCol >= 0 && lb.getRopeView(lr, oLeftCol, view) ? lookup.find(view) : o;
//            }
//            rebind(s, p, o);
//            try {
//                if (mustRebindModifier != null) {
//                    convTpEmitter.reset();
//                    rightEmitter = mustRebindModifier.processed(convTpEmitter, binding, false);
//                } else {
//                    rightEmitter.reset();
//                }
//            } catch (RebindUnsupportedException e) {
//                return Emitters.error(batchType, vars, e);
//            }
//            return rightEmitter;
//        }
//
//        private void rebind(long s, long p, long o) {
//            var e = tpEmitter;
//            switch (tpFreeRoles) {
//                case EMPTY       -> ((StoreAskEmitter)e).result = spo.contains(s, p, o);
//                case OBJ         -> spo.values (s, p, ((StoreValueEmitter) e).vit);
//                case PRE         -> spo.subKeys(s, o, ((StoreSubKeyEmitter)e).sit);
//                case PRE_OBJ     -> spo.pairs  (s,    ((StorePairEmitter)  e).pit);
//                case SUB         -> ops.values (o, p, ((StoreValueEmitter) e).vit);
//                case SUB_OBJ     -> pso.pairs  (p,    ((StorePairEmitter)  e).pit);
//                case SUB_PRE     -> ops.pairs  (o,    ((StorePairEmitter)  e).pit);
//                case SUB_PRE_OBJ -> spo.scan   (      ((StoreScanEmitter)  e).sit);
//            }
//        }
//
//        @Override protected @Nullable Emitter<B> continueRight() {
//            if (!hasLexicalJoin)
//                return null;
//            long s, p, o;
//            if (nextLexical(lb, lr)) {
//                s = sLexIt == null ? sNotLex : sLexIt.id;
//                p = pLexIt == null ? pNotLex : pLexIt.id;
//                o = oLexIt == null ? oNotLex : oLexIt.id;
//                rebind(s, p, o);
//                try {
//                    rightEmitter.reset();
//                } catch (RebindUnsupportedException e) {
//                    return Emitters.error(batchType, vars, e);
//                }
//                return rightEmitter;
//            } else {
//                lb = null;
//                return null;
//            }
//        }
//        private long lexRebindCol(LexIt lexIt, byte leftCol, long fallback, B lb, int lr) {
//            if (leftCol < 0) return fallback;
//            if (lexIt == null && lb instanceof StoreBatch sb) {
//                long sourced = sb.id(lr, leftCol);
//                if (IdTranslator.dictId(sourced) == dictId)
//                    return unsource(sourced);
//            }
//            if (!lb.getRopeView(lr, leftCol, view))
//                return fallback;
//            if (lexIt == null)
//                return lookup.find(view);
//            lexIt.find(view);
//            return lexIt.advance() ? lexIt.id : fallback;
//        }
//
//        private boolean resetLexIt(LexIt lexIt, byte leftCol, B lb, int lr) {
//            if (!lb.getRopeView(lr, leftCol, view)) return false;
//            lexIt.find(view);
//            return lexIt.advance();
//        }
//
//        private boolean nextLexical(B lb, int lr) {
//            if (oLexIt != null && oLexIt.advance())
//                return true;
//            if (pLexIt != null) {
//                while (pLexIt.advance()) {
//                    if (oLexIt == null || resetLexIt(oLexIt, oLeftCol, lb, lr)) return true;
//                }
//            }
//            if (sLexIt != null) {
//                while (sLexIt.advance()) {
//                    if (pLexIt != null) {
//                        if (!resetLexIt(pLexIt, pLeftCol, lb, lr)) continue; // next s
//                        if (oLexIt == null) return true;
//                        do {
//                            if (resetLexIt(oLexIt, oLeftCol, lb, lr)) return true;
//                        } while (pLexIt.advance());
//                    } else if (resetLexIt(oLexIt, oLeftCol, lb, lr)) {
//                        return true;
//                    }
//                }
//            }
//            return false;
//        }
//
//        private void endLexical() {
//            if (sLexIt != null) sLexIt.end();
//            if (pLexIt != null) pLexIt.end();
//            if (oLexIt != null) oLexIt.end();
//        }
//
//    }

    private final class StoreBindingEmitter<B extends Batch<B>> extends BindingStage<B> {
        private static final LocalityCompositeDict.LocalityLexIt [] EMPTY_LEX_ITS
                = new LocalityCompositeDict.LocalityLexIt[0];

        /** If a lexical join hold lexical variants of the left-provided binding using the
         *  var names of the right operand instead of the left-side binding var name. */
        private final @Nullable BatchBinding lexJoinBinding;
        /** Next values for {@code lexJoinBinding}, continuously swapped with
         * {@code lexJoinBinding.batch} */
        private @Nullable B nextLexBatch;
        /** The i-th lexIt iterates over variants of the {@code lexItCols[i]}-th binding var */
        private final int[] lexItsCols;
        /** array of non-null lexical variant iterators or null if there is no lexical join */
        private final LocalityCompositeDict.LocalityLexIt [] lexIts;
        /** used to convert ids from {@code lexIts} into values for {@code nextLexJoinBatch}. */
        private final LocalityCompositeDict.@Nullable Lookup lookup;
        /** shortcut bitset for checking if a column of {@code nextLexJoinBatch} should be
         * filled with a value from a {@code lexIt} instead of just copied from
         * {@code lexJoinBinding.batch}*/
        private final long lexBindingCols;
        private final @Nullable TwoSegmentRope lexView;
        /** A 1-row batch used to convert lexical binding batches that are not of type B */
        private @Nullable B convLeftBatch;

        public StoreBindingEmitter(EmitBindQuery<B> bq, Plan right,
                                   Vars rebindHint) {
            super(bq.bindings, bq.type, bq, bq.resultVars(),
                  emit(bq.bindings.batchType(), right,
                       bq.bindingsVars().union(rebindHint)));
            Vars leftVars = bq.bindingsVars();

            var m = right instanceof Modifier mod ? mod : null;
            List<Expr> mFilters = m == null ? List.of() : m.filters;
            lexJoinBinding = mFilters.isEmpty() ? null
                                : createLexJoinBinding(right, mFilters, leftVars);
            if (lexJoinBinding == null) {
                lexIts = EMPTY_LEX_ITS;
                lexItsCols = ArrayPool.EMPTY_INT;
                nextLexBatch = null;
                lookup = null;
                lexBindingCols = 0;
                lexView = null;
            } else { // fill lexIts & lexItsCols
                Vars rightBindingVars = lexJoinBinding.vars;
                int nVars = leftVars.size();
                assert rightBindingVars.size() == nVars;
                nextLexBatch = batchType.create(1, nVars, 0);
                lookup = dict.lookup();
                int lexJoins = 0;
                for (int i = 0; i < nVars; i++) {
                    if (leftVars.get(i) != rightBindingVars.get(i)) lexJoins++;
                }
                lexItsCols = new int[lexJoins];
                lexIts = new LocalityCompositeDict.LocalityLexIt[lexJoins];
                long lexBindingCols = 0;
                lexJoins = 0;
                for (int i = 0; i < nVars; i++) {
                    if (leftVars.get(i) == rightBindingVars.get(i)) continue;
                    if (i < 64) lexBindingCols |= 1L << i;
                    lexItsCols[lexJoins]   = i;
                    lexIts    [lexJoins++] = dict.lexIt();
                }
                this.lexBindingCols = lexBindingCols;
                lexView = new TwoSegmentRope();
            }
        }

        private BatchBinding createLexJoinBinding(Plan right, List<Expr> mFilters,
                                                     Vars leftVars) {
            Vars rightBindingVars = null;
            var term = Term.pooledMutable();
            var varRope = ByteRope.pooled(16);
            for (SegmentRope name : right.allVars()) {
                varRope.len = 1;
                varRope.append(name).u8()[0] = '?';
                term.set(ByteRope.EMPTY, varRope, false);
                Term leftVar = leftLexJoinVar(mFilters, term, leftVars);
                if (leftVar != term) {
                    if (rightBindingVars == null)
                        rightBindingVars = Vars.fromSet(leftVars, leftVars.size());
                    int i = rightBindingVars.indexOf(leftVar);
                    if (i >= 0)
                        rightBindingVars.set(i, name);
                }
            }
            varRope.recycle();
            term.recycle();
            if (rightBindingVars != null) {
                BatchBinding lexJoinBinding = new BatchBinding(rightBindingVars);
                B b = batchType().create(1, rightBindingVars.size(), 0);
                b.beginPut();
                b.commitPut();
                lexJoinBinding.attach(b, 0);
                return lexJoinBinding;
            }
            return null;
        }

        @Override protected void doRelease() {
            super.doRelease();
            if (lexJoinBinding != null) //noinspection unchecked
                lexJoinBinding.attach(batchType.recycle((B)lexJoinBinding.batch), 0);
            nextLexBatch = batchType.recycle(nextLexBatch);
            convLeftBatch = batchType.recycle(convLeftBatch);
        }

        @Override protected boolean lexContinueRight(BatchBinding binding,
                                                     Emitter<B> rightEmitter) {
            if (lexJoinBinding == null)
                return false;
            int nIts = lexIts.length, i = nIts-1;
            while (i >= 0 && !lexIts[i].advance())
                --i;
            if (i < 0)
                return false;

            //noinspection unchecked
            B lexBatch = (B)lexJoinBinding.batch, nextLexBatch = this.nextLexBatch;
            assert nextLexBatch != null && lookup != null && lexView != null && lexBatch != null;
            int nVars = lexBatch.cols;

            clearAndCopyNonLexBindings(lexBatch, 0, nextLexBatch);
            if (nVars > 64)
                copyNonLexBindingsExtra(nextLexBatch, 0, lexBatch);
            putLex(nextLexBatch, i, lookup);

            while (++i < nIts) {
                if (!binding.get(lexItsCols[i], lexView))
                    return false;
                var it = lexIts[i];
                it.find(lexView);
                if (!it.advance())
                    return false;
                putLex(nextLexBatch, i, lookup);
            }
            nextLexBatch.commitPut();
            this.nextLexBatch = lexBatch;
            lexJoinBinding.batch = nextLexBatch;
            rightEmitter.rebind(lexJoinBinding);
            return true;
        }

        private void clearAndCopyNonLexBindings(B src, int srcRow, B dst) {
            dst.clear();
            dst.beginPut();
            for (int c = 0, n = Math.min(64, dst.cols); c < n; c++) {
                if ((lexBindingCols & (1L << c)) == 0)
                    dst.putTerm(c, src, srcRow, c);
            }
        }

        private void copyNonLexBindingsExtra(B src, int srcRow, B dst) {
            outer:
            for (int c = 64, n = src.cols; c < n; c++) {
                for (int lexCol : lexItsCols) {
                    if (lexCol == c) continue outer;
                }
                dst.putTerm(c, src, srcRow, c);
            }
        }

        private void putLex(B nextLexBatch, int lexIdx, LocalityCompositeDict.Lookup lookup) {
            long id = lexIts[lexIdx].id;
            SegmentRope local = lookup.getLocal(id);
            if (local == null)
                return;
            nextLexBatch.putTerm(lexItsCols[lexIdx], lookup.getShared(id), local,
                                 0, local.len, lookup.sharedSuffixed(id));
        }

        @Override protected void rebind(BatchBinding binding, Emitter<B> rightEmitter) {
            if (lexJoinBinding != null) binding = lexRebind(binding, lexJoinBinding);
            rightEmitter.rebind(binding);
        }

        private BatchBinding lexRebind(BatchBinding binding, BatchBinding lexJoinBinding) {
            Batch<?> bBatch = binding.batch;
            if (bBatch == null)
                throw new IllegalArgumentException("invalid binding");
            B leftBatch;
            if (bBatch.type().equals(batchType)) {//noinspection unchecked
                leftBatch = (B)bBatch;
            } else {
                leftBatch = batchType.empty(convLeftBatch, 1, bBatch.cols, 0);
                convLeftBatch = leftBatch = leftBatch.putConverting(bBatch);
            }

            @SuppressWarnings("unchecked") B lexBatch = (B)lexJoinBinding.batch;
            assert lexBatch != null && lexView != null && lookup != null;
            int leftRow = binding.row, nVars = leftBatch.cols;
            clearAndCopyNonLexBindings(leftBatch, leftRow, lexBatch);
            if (nVars > 64)
                copyNonLexBindingsExtra(leftBatch, leftRow, lexBatch);
            for (int i = 0; i < lexIts.length; i++) {
                if (leftBatch.getRopeView(leftRow, lexItsCols[i], lexView)) {
                    var it = lexIts[i];
                    it.find(lexView);
                    if (it.advance())
                        putLex(lexBatch, i, lookup);
                    else
                        lexBatch.putTerm(lexItsCols[i], leftBatch, leftRow, lexItsCols[i]);
                }
            }
            binding = lexJoinBinding;
            return binding;
        }
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

        @Override protected StoreBatch fetch(StoreBatch dest) {
            if (vit.advance()) {
                dest.beginPut();
                if (term0Col >= 0) dest.putTerm(term0Col, source(vit.valueId, dictId));
                dest.commitPut();
            } else {
                exhausted = true;
            }
            return dest;
        }
    }

    final class StorePairBIt extends StoreIteratorBIt {
        private final Triples.PairIt pit;

        public StorePairBIt(Vars pubVars, Term subKeyVar, Term valueVar, Triples.PairIt pit) {
            super(pubVars, pubVars.indexOf(subKeyVar), pubVars.indexOf(valueVar));
            this.pit = pit;
        }

        @Override protected StoreBatch fetch(StoreBatch dest) {
            if (pit.advance()) {
                dest.beginPut();
                if (term0Col >= 0) dest.putTerm(term0Col, source(pit.subKeyId, dictId));
                if (term1Col >= 0) dest.putTerm(term1Col, source(pit.valueId, dictId));
                dest.commitPut();
            } else {
                exhausted = true;
            }
            return dest;
        }
    }

    final class StoreSubKeyBIt extends StoreIteratorBIt {
        private final Triples.SubKeyIt sit;

        public StoreSubKeyBIt(Vars pubVars, Term subKeyVar, Triples.SubKeyIt sit) {
            super(pubVars, pubVars.indexOf(subKeyVar), -1);
            this.sit = sit;
        }

        @Override protected StoreBatch fetch(StoreBatch dest) {
            if (sit.advance()) {
                dest.beginPut();
                if (term0Col >= 0) dest.putTerm(term0Col, source(sit.subKeyId, dictId));
                dest.commitPut();
            } else {
                exhausted = true;
            }
            return dest;
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

        @Override protected StoreBatch fetch(StoreBatch dest) {
            if (sit.advance()) {
                dest.beginPut();
                if (sCol >= 0) dest.putTerm(sCol, source(sit.keyId, dictId));
                if (pCol >= 0) dest.putTerm(pCol, source(sit.subKeyId, dictId));
                if (oCol >= 0) dest.putTerm(oCol, source(sit.valueId, dictId));
                dest.commitPut();
            } else {
                exhausted = true;
            }
            return dest;
        }
    }

    private static final class BindingNotifier {
        private final ItBindQuery<?> bindQuery;
        private long seq = -1;
        private boolean notified = false;

        private BindingNotifier(ItBindQuery<?> bindQuery) {
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

    private final class StoreBindingBIt<B extends Batch<B>> extends AbstractBIt<B> {
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
                rightFilter.rebindAcquire();
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

        @Override protected void cleanup(@Nullable Throwable cause) {
            try {
                // close() and failures from nextBatch() are rare. Rarely generating garbage is
                // cheaper than precisely tracking whether lb ownership is with this or with left,
                if (cause == null) {
                    batchType.recycle(lb);
                    lb = null; // signals exhaustion to nextBatch()
                }
                // if we arrived here from close(), nextBatch() may be concurrently executing.
                // it is cheaper to leak rb and fb than to synchronize
                if (!(cause instanceof BItClosedAtException)) {
                    rb = batchType.recycle(rb);
                    fb = batchType.recycle(fb);
                }
                if (rightFilter != null) {
                    rightFilter.rebindRelease();
                    rightFilter.release();
                }
            } finally {
                try {
                    super.cleanup(cause);
                } finally {
                    releaseRef();
                }
            }
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
                else             onNextBatch(b);
            } catch (Throwable t) {
                if (state() == State.ACTIVE)
                    onTermination(t);
                lb = null; // signal exhaustion
                throw t;
            }
            return b;
        }

        @SuppressWarnings("SameReturnValue") private B handleEmptyBatch(B batch) {
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
                case SUB_PRE_OBJ -> spo.scan   (      (Triples.ScanIt)  rIt);
                case OBJ         -> spo.values (s, p, (Triples.ValueIt) rIt);
                case PRE         -> spo.subKeys(s, o, (Triples.SubKeyIt)rIt);
                case PRE_OBJ     -> spo.pairs  (s,    (Triples.PairIt)  rIt);
                case SUB         -> ops.values (o, p, (Triples.ValueIt) rIt);
                case SUB_OBJ     -> pso.pairs  (p,    (Triples.PairIt)  rIt);
                case SUB_PRE     -> ops.pairs  (o,    (Triples.PairIt)  rIt);
            }
            if (rightFilter != null)
                rightFilter.rebind(BatchBinding.ofEmpty(batchType));
            if (metrics instanceof Metrics.JoinMetrics jm) jm.beginBinding();
        }
    }
}
