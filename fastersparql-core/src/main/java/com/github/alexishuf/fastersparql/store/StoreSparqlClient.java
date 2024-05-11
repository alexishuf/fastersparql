package com.github.alexishuf.fastersparql.store;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItCancelledException;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.SingletonBIt;
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
import com.github.alexishuf.fastersparql.emit.Stage;
import com.github.alexishuf.fastersparql.emit.async.AsyncStage;
import com.github.alexishuf.fastersparql.emit.async.EmitterService;
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
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.PooledTermView;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.store.batch.IdTranslator;
import com.github.alexishuf.fastersparql.store.batch.StoreBatch;
import com.github.alexishuf.fastersparql.store.batch.StoreBatchType;
import com.github.alexishuf.fastersparql.store.index.dict.LexIt;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityCompositeDict;
import com.github.alexishuf.fastersparql.store.index.triples.Triples;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.*;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.model.TripleRoleSet.EMPTY;
import static com.github.alexishuf.fastersparql.model.TripleRoleSet.*;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.operators.plan.Operator.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Type.VAR;
import static com.github.alexishuf.fastersparql.store.batch.IdTranslator.*;
import static com.github.alexishuf.fastersparql.store.index.dict.Dict.NOT_FOUND;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.*;
import static com.github.alexishuf.fastersparql.util.concurrent.ResultJournal.rebindEmitter;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static com.github.alexishuf.fastersparql.util.owned.Orphan.takeOwnership;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Arrays.copyOfRange;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;

public class StoreSparqlClient extends AbstractSparqlClient
                               implements CardinalityEstimatorProvider {
    private static final Logger log = LoggerFactory.getLogger(StoreSparqlClient.class);
    private static final StoreBatchType TYPE = StoreBatchType.STORE;
    private static final boolean PREFER_NATIVE = FSProperties.storePreferIds();
    private static final int PREFIXES_MASK = -1 >>> Integer.numberOfLeadingZeros(
            (8*1024*1024)/(4/* SegmentRope ref */ + 32/* SegmentRope obj */));
    private static final LIFOPool<FinalSegmentRope[]> PREFIXES_POOL = new LIFOPool<>(
            FinalSegmentRope[].class, "StoreSparqlClient.PREFIXES_POOL", 16,
            16/*obj*/ + 2*4/*Rope*/ + 8+2*4/*SegmentRope*/ + 2*4 /*SegmentRopeView*/);

    private final LocalityCompositeDict dict;
    private final int dictId;
    private final Triples spo, pso, ops;
    private final SingletonFederator federator;
    private final FinalSegmentRope[] prefixes;
    private final boolean hugeDict;

    /* --- --- --- lifecycle --- --- --- */

    public StoreSparqlClient(SparqlEndpoint ep) {
        super(ep);
        FinalSegmentRope[] prefixes = PREFIXES_POOL.get();
        this.prefixes = prefixes == null ? new FinalSegmentRope[PREFIXES_MASK+1] : prefixes;
        this.bindingAwareProtocol = true;
        this.cheapestDistinct = DistinctType.WEAK;
        this.localInProcess = true;
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
        this.hugeDict = dict.strings() > 40_000_000;
        log.debug("Loaded{} {}...", validate ? "/validated" : "", dir);
    }

    private static Triples loadTriples(Path path, boolean validate) throws IOException {
        Triples t = new Triples(path);
        if (validate)
            t.validate();
        return t;
    }

    private static BatchType<?> maybeNative(BatchType<?> requested) {
        return PREFER_NATIVE ? TYPE : requested;
    }

    private static void appendToSimpleLabel(StringBuilder sb, SparqlEndpoint endpoint,
                                            TriplePattern tp) {
        String uri = endpoint.uri();
        int begin = uri.lastIndexOf('/');
        String file = begin < 0 ? uri : uri.substring(begin);
        sb.append('\n').append(tp).append(file);
    }

    public class Guard extends RefGuard {
        public Guard() { }
        public StoreSparqlClient get() { return StoreSparqlClient.this; }
    }

    public Guard retain() { return new Guard(); }

    @Override protected void doClose() {
        if (ThreadJournal.ENABLED)
            journal("Closing dictId=", dictId, "endpoint=", endpoint);
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
        private final Triples spo, pso, ops;
        private final int avgS, avgP, avgO, avgSP, avgSO, avgPO;
        private final float invAvgSP, invAvgSO, invAvgPO;

        public StoreSingletonFederator(StoreSparqlClient parent) {
            super(parent, StoreSparqlClient.PREFER_NATIVE ? StoreSparqlClient.TYPE : null);
            this.spo    = parent.spo;
            this.pso    = parent.pso;
            this.ops    = parent.ops;
            var compute = ForkJoinPool.commonPool().invoke(new ComputeAvgPairs());
            this.avgS   = compute.avgS;
            this.avgP   = compute.avgP;
            this.avgO   = compute.avgO;
            this.avgSP  = compute.avgSP;
            this.avgSO  = compute.avgSO;
            this.avgPO  = compute.avgPO;
            invAvgSP    = 1/(float)avgSP;
            invAvgSO    = 1/(float)avgSO;
            invAvgPO    = 1/(float)avgPO;
        }

        private class ComputeAvgPairs extends RecursiveTask<ComputeAvgPairs> {
            int avgS, avgP, avgO, avgSP, avgSO, avgPO;
            @Override protected ComputeAvgPairs compute() {
                var spoTask = new ForPermutation(spo);
                var psoTask = new ForPermutation(pso);
                var opsTask = new ForPermutation(ops);
                invokeAll(spoTask, psoTask, opsTask);
                avgS  = spoTask.avgPairs;
                avgP  = psoTask.avgPairs;
                avgO  = opsTask.avgPairs;
                avgPO = spoTask.avgKeys;
                avgSO = psoTask.avgKeys;
                avgSP = opsTask.avgKeys;
                return this;
            }

            private static final class ForPermutation extends RecursiveAction {
                private final Triples triples;
                int avgPairs, avgKeys;
                public ForPermutation(Triples triples) {this.triples = triples;}
                @Override protected void compute() {
                    long sum = 0, keysCount = 0;
                    for (long k = triples.firstKey(), end = k+triples.keysCount(); k < end; k++) {
                        long pairs = triples.estimatePairs(k);
                        if (pairs > 0) {
                            ++keysCount;
                            sum += pairs;
                        }
                    }
                    double keysD = (double)keysCount;
                    long triplesCount = triples.triplesCount();
                    this.avgPairs = (int)min(I_MAX, max(1, sum         /keysD));
                    this.avgKeys  = (int)min(I_MAX, max(1, triplesCount/keysD));
                }
            }
        }

        @Override public int estimate(TriplePattern t, @Nullable Binding binding) {
            long s, p, o;
            var vars = binding == null ? t.freeRoles() : t.freeRoles(binding);
            var lookup = dict.lookup().takeOwnership(this);
            try {
                s = t.s.type() == VAR ? 0 : lookup.find(t.s);
                p = t.p.type() == VAR ? 0 : lookup.find(t.p);
                o = t.o.type() == VAR ? 0 : lookup.find(t.o);
                if (vars == EMPTY)
                    return 1; // short circuit for ask queries
            } finally {
                lookup.recycle(this);
            }

            // we cannot delegate estimation if we have a GROUND dummy term. Doing so would get
            // estimate == 0 even for expensive queries. When returning an avg*, field is not
            // enough, estimation relies on weighing avgS/avgP/avgO by an estimatePairs() of
            // another non-dummy grounded term. The following switch is not the prettiest, but
            // it avoids having 7 call sites to estimatePairs(). Too many call sites are a slight
            // perf issue as the JIT will give up inlining or will output huge code
            var dummies = t.dummyRoles(binding);
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
                        dummyWeight = min(dummyWeight, spo.estimatePairs(s));
                    yield (int)(1 + avgP * dummyWeight);
                }
            };
            return (int) min(I_MAX, estimate);
        }

        @SuppressWarnings({"unchecked", "unused"}) @Override
        protected <I extends Batch<I>, B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
        convert(BatchType<B> dst, Orphan<? extends Emitter<?, ?>> in) {
            return converterFromStore(dst, (Orphan<? extends Emitter<StoreBatch, ?>>) in);
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
        var l = meat == null ? null : dict.lookup().takeOwnership(this);
        try {
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
        } finally {
            if (l != null)
                l.recycle(this);
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
                    var b = TYPE.create(vars.size()).takeOwnership(this);
                    b.beginPut();
                    b.commitPut();
                    yield new SingletonBIt<>(b.releaseOwnership(this), TYPE, vars);
                }
                yield new EmptyBIt<>(TYPE, vars);
            }
            case OBJ         -> new  StoreValueBIt(vars, t, t.o,          spo. values(si, pi));
            case PRE         -> new StoreSubKeyBIt(vars, t, t.p,          spo.subKeys(si, oi));
            case PRE_OBJ     -> new   StorePairBIt(vars, t, t.p, t.o,     spo.  pairs(si));
            case SUB         -> new  StoreValueBIt(vars, t, t.s,          ops. values(oi, pi));
            case SUB_OBJ     -> new   StorePairBIt(vars, t, t.s, t.o,     pso.  pairs(pi));
            case SUB_PRE     -> new   StorePairBIt(vars, t, t.p, t.s,     ops.  pairs(oi));
            case SUB_PRE_OBJ -> new    StoreScanIt(vars, t);
        };
    }

    @SuppressWarnings("unchecked") @Override
    public <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    doEmit(BatchType<B> bt, SparqlQuery sparql, Vars rebindHint) {
        Plan plan = SparqlParser.parse(sparql);
        var tp = (plan.type == MODIFIER ? plan.left : plan) instanceof TriplePattern t ? t : null;
        Modifier m = plan instanceof Modifier mod ? mod : null;
        boolean convertBefore = m != null && !m.filters.isEmpty() && bt != TYPE;
        Orphan<? extends Emitter<?, ?>> em;
        if (tp != null) {
            Vars tpVars = (m != null && m.filters.isEmpty() ? m : tp).publicVars();
            var tpEm = new TPEmitter(tp, tpVars);
            em = tpEm;
            if (convertBefore)
                em = converterFromStore(bt, tpEm);
            if (m != null) // apply any modification required (projection may be done already)
                em = m.processed((Orphan<? extends Emitter<B,?>>)em);
        } else {
            if (convertBefore)
                plan = plan.left();
            em = federator.emit(maybeNative(bt), plan, rebindHint);
            if (convertBefore) {
                var conv = converterFromStore(bt, (Orphan<? extends Emitter<StoreBatch, ?>>)em);
                em =  m.processed(conv);
            }
        }
        return bt.equals(Emitter.peekBatchTypeWild(em))
                ? (Orphan<? extends Emitter<B, ?>>) em
                : converterFromStore(bt, (Orphan<? extends Emitter<StoreBatch, ?>>)em);
    }

    @Override public <B extends Batch<B>> BIt<B> doQuery(ItBindQuery<B> bq) {
        Plan right = bq.query instanceof Plan p ? p : SparqlParser.parse(bq.query);
        if (bq.query.isGraph())
            throw new InvalidSparqlQueryType("query() method only takes SELECT/ASK queries");
        Vars outVars = bq.resultVars();
        BIt<B> it;
        var l = dict.lookup().takeOwnership(this);
        try {
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
        } finally {
            l.recycle(this);
        }
        return it;
    }

    @Override public <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    doEmit(EmitBindQuery<B> bq, Vars rebindHint) {
        Plan right = bq.query instanceof Plan p ? p : SparqlParser.parse(bq.query);
        if (right instanceof Modifier m0 && !m0.filters.isEmpty())
            right = splitFiltersForLexicalJoin(m0);
        return new StoreBindingEmitter<>(bq, right, rebindHint);
    }

    private Modifier splitFiltersForLexicalJoin(Modifier m) {
        var m2 = (Modifier)m.copy();
        m2.filters = Optimizer.splitFilters(m2.filters);
        return m2;
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
        int n             = join.opCount();
        var left          = bq.bindings;
        var bindNotifier  = new BindingNotifier(bq);
        var weakDedup = outerMod != null && outerMod.distinct != null && outerMod.offset == 0
                      ? DistinctType.WEAK : null;
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
                    op = modifyLastOperand(outerMod.filters, weakDedup, op);
                vars = outVars;
                opBindNotifier = bindNotifier;
            } else if (weakDedup != null) {
                op = FS.distinct(op, weakDedup);
            }
            left = new StoreBindingBIt<>(left, BindType.JOIN, op, tp, s, p, o, vars,
                                         i == 0 ? bindNotifier : null, opBindNotifier, lookup);
        }
        if (outerMod != null && (outerMod.limit != Long.MAX_VALUE || outerMod.offset > 0
                || DistinctType.compareTo(outerMod.distinct, weakDedup) > 0)) {
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

    private Plan modifyLastOperand(List<Expr> filters, DistinctType distinctType, Plan op) {
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
        return new Modifier(op, null, distinctType, 0, Long.MAX_VALUE, filters);
    }

    /* --- --- --- emitters --- --- --- */

//    @SuppressWarnings("unused") private static int plainRepeatRebind;
//    public static final VarHandle REPEAT_REBIND;
//    static {
//        try {
//            REPEAT_REBIND = MethodHandles.lookup().findStaticVarHandle(StoreSparqlClient.class, "plainRepeatRebind", int.class);
//        } catch (NoSuchFieldException|IllegalAccessException e) {
//            throw new ExceptionInInitializerError(e);
//        }
//    }

    private static abstract sealed class PrefetchTask extends EmitterService.Task<PrefetchTask> {
        private static final byte CHUNK_ROWS = 32;

//        private static final VarHandle REQUEST, NEXT_ROW;
//        static {
//            try {
//                REQUEST = MethodHandles.lookup().findVarHandle(PrefetchTask.class, "request", Batch.class);
//                NEXT_ROW = MethodHandles.lookup().findVarHandle(PrefetchTask.class, "nextRow", int.class);
//            } catch (NoSuchFieldException | IllegalAccessException e) {
//                throw new ExceptionInInitializerError(e);
//            }
//        }

        private Batch<?> requestedBindingBatch;
        private BatchBinding binding;
        private volatile int asyncDone, asyncBottom;
        private final short dictId;
        byte unsrcIdsCols, sOutCol, pOutCol, oOutCol;
        private byte sInCol, pInCol, oInCol, rowSkelCols;
        final long sId, pId, oId;
        private final LocalityCompositeDict.Lookup syncLookup, asyncLookup;
        private final TwoSegmentRope syncView, asyncView;
        long[] unsrcIds;
        private final TPEmitter tpEmitter;
        private short[] skelCol2InCol = EMPTY_SHORT;
        private long [] rowSkels      = EMPTY_LONG;

        private PrefetchTask(short dictId, LocalityCompositeDict dict, TPEmitter tpEmitter) {
            super(EMITTER_SVC, RR_WORKER, CREATED, TASK_FLAGS);
            this.dictId       = dictId;
            this.syncLookup   = dict.lookup().takeOwnership(this);
            this.asyncLookup  = dict.lookup().takeOwnership(this);
            this.syncView     = new TwoSegmentRope();
            this.asyncView    = new TwoSegmentRope();
            this.unsrcIds     = longsAtLeast(TYPE.preferredTermsPerBatch()>>1);
            this.tpEmitter    = tpEmitter;
            this.sId          = asyncLookup.find(tpEmitter.tp.s);
            this.pId          = asyncLookup.find(tpEmitter.tp.p);
            this.oId          = asyncLookup.find(tpEmitter.tp.o);
        }

        private static final class Concrete extends PrefetchTask implements Orphan<PrefetchTask> {
            private Concrete(short dictId, LocalityCompositeDict dict, TPEmitter tpEmitter) {
                super(dictId, dict, tpEmitter);
            }
            @Override public PrefetchTask takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override protected void doRelease() {
            binding   = null;
            syncLookup .recycle(this);
            asyncLookup.recycle(this);
            unsrcIds      = recycleLongsAndGetEmpty(unsrcIds);
            rowSkels      = recycleLongsAndGetEmpty(rowSkels);
            skelCol2InCol = recycleShortsAndGetEmpty(skelCol2InCol);
            super.doRelease();
        }

        @Override public String journalName() {
            return "Store$PrefetchTask@"+Integer.toHexString(System.identityHashCode(this));
        }

        void setInCols(int sInCol, int pInCol, int oInCol) {
            if (sInCol > 0x7f || pInCol > 0x7f || oInCol > 0x7f)
                throw new IllegalArgumentException("Too many input columns");
            this.sInCol = (byte) sInCol;
            this.pInCol = (byte) pInCol;
            this.oInCol = (byte) oInCol;
            byte cols = 0;
            this.sOutCol = sInCol < 0 ? -1 : cols++;
            this.pOutCol = pInCol < 0 ? -1 : cols++;
            this.oOutCol = oInCol < 0 ? -1 : cols++;
            this.unsrcIdsCols = cols;
        }

        void setupBindSkel(boolean bindSkel, Vars outVars, Vars bindingVars) {
            if (bindSkel) {
                int outCols = outVars.size();
                rowSkelCols = (byte)outCols;
                if (skelCol2InCol.length < outCols)
                    skelCol2InCol = shortsAtLeast(outCols);
                for (int c = 0; c < outCols; c++)
                    skelCol2InCol[c] = (short)bindingVars.indexOf(outVars.get(c));
            } else {
                rowSkelCols = 0;
            }
        }

        void request(BatchBinding binding) {
            short bottom = (short) max(1, binding.row);
            Batch<?> bb = binding.batch;
            short rows = bb == null ? 0 : bb.rows;
            short snapshot = stop();
            try {
                if (rows*unsrcIdsCols > unsrcIds.length)
                    unsrcIds = longsAtLeast(rows*unsrcIdsCols, unsrcIds);
                if (rows*rowSkelCols > rowSkels.length)
                    rowSkels = longsAtLeast(rows*rowSkelCols, rowSkels);
                this.requestedBindingBatch = bb;
                this.binding               = binding;
                asyncDone = rows;
                asyncBottom = bottom;
            } finally {
                if (!allowRun(snapshot) && rows >= CHUNK_ROWS>>1)
                    awake();
            }
        }

        int awaitRow(BatchBinding binding) {
            short row = binding.row;
            Batch<?> batch = binding.batch;
            if (this.binding == binding && requestedBindingBatch == batch) {
                asyncBottom = row+1;
                if (batch != null && row == batch.rows-1)
                    allowRun(disallowRun());
                if (asyncDone <= row)
                    return row; // row fetched asynchronously
            } else {
                request(binding);
            }
            fetchRow(row);
            return 0;
        }

        short stop() {
            short snapshot        = disallowRun();
            binding               = null;
            requestedBindingBatch = null;
            asyncBottom = Integer.MAX_VALUE;
            return snapshot;
        }

        @Override protected void onPendingRelease() {
            short snapshot = stop();
            try {
                if (moveStateRelease(statePlain(), COMPLETED))
                    markDelivered(COMPLETED);
            } finally {
                allowRun(snapshot);
            }
        }

        @SuppressWarnings("DataFlowIssue")
        private long toUnsourcedId(int col, int row, BatchBinding binding,
                                   LocalityCompositeDict.Lookup lookup, TwoSegmentRope view) {
            Batch<?> batch = binding.batch;
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

        private void fetchRow(int r) {
            BatchBinding b = this.binding;
            if (sInCol >= 0)
                unsrcIds[sOutCol] = toUnsourcedId(sInCol, r, b, syncLookup, syncView);
            if (pInCol >= 0)
                unsrcIds[pOutCol] = toUnsourcedId(pInCol, r, b, syncLookup, syncView);
            if (oInCol >= 0)
                unsrcIds[oOutCol] = toUnsourcedId(oInCol, r, b, syncLookup, syncView);
            if (rowSkelCols != 0) {
                short[] skelCol2InCol = this.skelCol2InCol;
                for (int c = 0; c < rowSkelCols; c++) {
                    int bc = skelCol2InCol[c];
                    rowSkels[c] = bc < 0 ? NOT_FOUND
                            : source(toUnsourcedId(bc, r, b, syncLookup, syncView), dictId);
                }
            }
        }

        @Override protected void task(int threadId) {
            short r = (short)(asyncDone-1);
            var b = this.binding;
            if (b == null || r < 0)
                return;
            long[] rowSkels = this.rowSkels;
            long[] unsrcIds = this.unsrcIds;
            byte  sInCol = this. sInCol,  pInCol = this. pInCol,  oInCol = this. oInCol;
            byte sOutCol = this.sOutCol, pOutCol = this.pOutCol, oOutCol = this.oOutCol;
            byte outCols = this.unsrcIdsCols;
            short base = (short)(r*outCols);
            byte rowSkelCols = this.rowSkelCols, i = 0;
            for (; i < CHUNK_ROWS && r >= asyncBottom; ++i, base-=outCols) {
                if (sInCol >= 0)
                    unsrcIds[base+sOutCol] = toUnsourcedId(sInCol, r, b, asyncLookup, asyncView);
                if (pInCol >= 0)
                    unsrcIds[base+pOutCol] = toUnsourcedId(pInCol, r, b, asyncLookup, asyncView);
                if (oInCol >= 0)
                    unsrcIds[base+oOutCol] = toUnsourcedId(oInCol, r, b, asyncLookup, asyncView);
                if (rowSkelCols > 0) {
                    int rsBase = r*rowSkelCols;
                    short[] skelCol2InCol = this.skelCol2InCol;
                    for (int c = 0; c < rowSkelCols; c++) {
                        int bc = skelCol2InCol[c];
                        rowSkels[rsBase+c] = bc < 0 ? NOT_FOUND
                                : source(toUnsourcedId(bc, r, b, asyncLookup, asyncView), dictId);
                    }
                }
                asyncDone = r--;
            }
            if (r >= asyncBottom) {
                avoidWorker(tpEmitter);
                awake();
            }
        }
    }

    private final class TPEmitter extends TaskEmitter<StoreBatch, TPEmitter>
            implements Orphan<TPEmitter> {
        private static final int LIMIT_TICKS       = 1;
        private static final int DEADLINE_CHK      = 0x3f;
        private static final int HAS_UNSET_OUT     = 0x01000000;
        private static final Flags FLAGS = TASK_FLAGS.toBuilder()
                .flag(HAS_UNSET_OUT, "HAS_UNSET_OUT")
                .build();

        private final byte cols;
        private byte outCol0, outCol1, outCol2;
        private final short dictId = (short)StoreSparqlClient.this.dictId;
        private byte freeRoles;
        //private byte yields;
        private boolean retry;
        private Object it;
        private long[] rowSkels = EMPTY_LONG;
        private int rowSkelBegin;
        // ---------- fields below this line are accessed only on construction/rebind()
        private final PrefetchTask pref;
        private int lastRebindSeq = -1;
        private final TriplePattern tp;
        private @MonotonicNonNull Vars lastBindingsVars;
        private final Vars bindableVars;

        public TPEmitter(TriplePattern tp, Vars outVars) {
            super(TYPE, outVars, EMITTER_SVC, RR_WORKER, CREATED, FLAGS);
            int cols = outVars.size();
            if (cols > 127)
                throw new IllegalArgumentException("Too many output columns");
            this.cols         = (byte) cols;
            this.tp           = tp;
            this.bindableVars = tp.allVars();
            this.pref = new PrefetchTask.Concrete(dictId, dict, this).takeOwnership(this);
            BatchBinding empty = BatchBinding.ofEmpty(TYPE);
            rebindPrefetch(empty);
            rebind(empty);
            rebindPrefetchEnd();
            if (stats != null)
                stats.rebinds = 0;
            if (ResultJournal.ENABLED)
                ResultJournal.initEmitter(this, vars);
            acquireRef();
        }

        @Override protected void doRelease() {
            try {
                pref.recycle(this);
                releaseRef();
            } finally {
                super.doRelease();
            }
        }

        @Override public TPEmitter takeOwnership(Object o) {return takeOwnership0(o);}

        @Override public boolean cancel() {
            boolean cancelled;
            lock();
            try {
                rebindPrefetchEnd();
            } finally {
                unlock();
                cancelled = super.cancel();
            }
            return cancelled;
        }

        private void bindingVarsChanged(Vars bindingVars) {
            if (ENABLED)
                journal("bindingVarsChanged bindingVars", bindingVars, "em=", this);
            lastBindingsVars = bindingVars;
            freeRoles = 0;
            int sInCol = -1, pInCol = -1, oInCol = -1;
            if (tp.s.isVar() && (sInCol = bindingVars.indexOf(tp.s)) < 0) freeRoles |= SUB_BITS;
            if (tp.p.isVar() && (pInCol = bindingVars.indexOf(tp.p)) < 0) freeRoles |= PRE_BITS;
            if (tp.o.isVar() && (oInCol = bindingVars.indexOf(tp.o)) < 0) freeRoles |= OBJ_BITS;
            short snapshot = pref.stop();
            try {
                pref.setInCols(sInCol, pInCol, oInCol);
                byte sc = (byte)vars.indexOf(tp.s);
                byte pc = (byte)vars.indexOf(tp.p);
                byte oc = (byte)vars.indexOf(tp.o);
                byte o0 = -1, o1 = -1, o2 = -1;
                switch (freeRoles) {
                    case       EMPTY_BITS ->  it = FALSE;
                    case         OBJ_BITS -> {it = spo.makeValuesIt(); o0 = oc;}
                    case         PRE_BITS -> {it = spo.makeSubKeyIt(); o0 = pc;}
                    case     PRE_OBJ_BITS -> {it = spo.makePairIt();   o0 = pc; o1 = oc;}
                    case         SUB_BITS -> {it = ops.makeValuesIt(); o0 = sc;}
                    case     SUB_OBJ_BITS -> {it = pso.makePairIt();   o0 = sc; o1 = oc;}
                    case     SUB_PRE_BITS -> {it = ops.makePairIt();   o0 = pc; o1 = sc;}
                    case SUB_PRE_OBJ_BITS -> {it = spo.scan();         o0 = sc; o1 = pc; o2 = oc;}
                }
                outCol0 = o0;
                outCol1 = o1;
                outCol2 = o2;
                int colsSet = 0;
                if (o0 != -1                        ) ++colsSet;
                if (o1 != -1 && o1 != o0            ) ++colsSet;
                if (o2 != -1 && o2 != o0 && o2 != o1) ++colsSet;
                pref.setupBindSkel(colsSet < cols, vars, bindingVars);
                if (colsSet < cols)
                    setFlagsRelease(HAS_UNSET_OUT);
                else
                    clearFlagsRelease(HAS_UNSET_OUT);
            } finally {
                pref.allowRun(snapshot);
            }
        }

        @Override public void rebindPrefetchEnd() {
            pref.allowRun(pref.stop());
        }

        @Override public void rebindPrefetch(BatchBinding binding) {
            int st = lock();
            try {
                if ((st&STATE_MASK) != CREATED && (st & IS_TERM) == 0)
                    return; // not rebind()able
                if (!binding.vars.equals(lastBindingsVars))
                    bindingVarsChanged(binding.vars);
                pref.request(binding);
            } finally { unlock(); }
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            if (binding.sequence == lastRebindSeq)
                return; //duplicate rebind() due to diamond in emitter graph
            lastRebindSeq = binding.sequence;
            Batch<?> bb = binding.batch;
            short bRow = binding.row;
            if (bb == null || bRow >= bb.rows)
                throw new IllegalArgumentException("invalid binding");
            Vars bVars = binding.vars;
            resetForRebind(0, LOCKED_MASK);
            try {
                if (EmitterStats.ENABLED && stats != null) stats.onRebind(binding);
                if (ResultJournal.ENABLED)                 rebindEmitter(this, binding);
                if (!bVars.equals(lastBindingsVars))       bindingVarsChanged(bVars);

                int dstRow = pref.awaitRow(binding);
                int base = pref.unsrcIdsCols*dstRow;
                rowSkels     = pref.rowSkels;
                rowSkelBegin = cols*dstRow;
                long[] unsourcedIds = pref.unsrcIds;
                long s = pref.sOutCol < 0 ? pref.sId : unsourcedIds[base+pref.sOutCol];
                long p = pref.pOutCol < 0 ? pref.pId : unsourcedIds[base+pref.pOutCol];
                long o = pref.oOutCol < 0 ? pref.oId : unsourcedIds[base+pref.oOutCol];
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
                unlock();
            }
        }

        @Override public Vars bindableVars() { return bindableVars; }

        @Override public String toString() { return super.toString()+'('+tp+')'; }

        @Override protected void appendToSimpleLabel(StringBuilder out) {
            StoreSparqlClient.appendToSimpleLabel(out, endpoint, tp);
        }

        @Override protected int produceAndDeliver(int state) {
            long limit = requested();
            if (limit <= 0)
                return state;

            var b = Orphan.takeOwnership(TYPE.createForThread(threadId, cols), this);
//            var b = Orphan.takeOwnership(TYPE.pollForThread(threadId, cols), this);
//            if (b == null) {
//                if (++yields < 4)
//                    return state;
//                yields = 0;
//                b = TYPE.createForThread(threadId, cols).takeOwnership(this);
//            }
            short sLimit = (short) min(b.termsCapacity/ max(1, cols), limit);
            retry = false;
            switch (freeRoles) {
                case                             EMPTY_BITS ->    fillAsk(b);
                case                      OBJ_BITS,SUB_BITS ->  fillValue(b, sLimit);
                case                               PRE_BITS -> fillSubKey(b, sLimit);
                case PRE_OBJ_BITS,SUB_OBJ_BITS,SUB_PRE_BITS ->   fillPair(b, sLimit);
                case                       SUB_PRE_OBJ_BITS ->   fillScan(b, sLimit);
            }
            int rows = b.rows; // fill*() only fills up to b.arr.length
            if (rows > 0) {
                journal("produced rows=", rows, "upd req=", requested());
                deliver(b.releaseOwnership(this));
            } else {
                b.recycle(this);
            }
            return retry ? state : COMPLETED;
        }

        @SuppressWarnings("SameReturnValue") private void fillAsk(StoreBatch b) {
            if (it == TRUE) {
                if ((statePlain()&HAS_UNSET_OUT) != 0)
                    arraycopy(rowSkels, rowSkelBegin, b.arr, 0, cols);
                b.rows = 1;
            }
        }

        private void fillValue(StoreBatch b, short limit) {
            short rows = 0;
            long[] a = b.arr;
            long deadline = Timestamp.nextTick(LIMIT_TICKS);
            boolean hasUnset = (statePlain()&HAS_UNSET_OUT) != 0;
            byte outCol0 = this.outCol0;
            var it = (Triples.ValueIt)this.it;
            for (int base = 0; rows < limit && (retry = it.advance()); base += cols) {
                if (hasUnset     ) arraycopy(rowSkels, rowSkelBegin, a, base, cols);
                if (outCol0  >= 0) a[base+outCol0] = source(it.valueId, dictId);
                ++rows;
                if ((rows&DEADLINE_CHK) == DEADLINE_CHK && Timestamp.nanoTime() > deadline) break;
            }
            b.rows = rows;
        }

        private void fillPair(StoreBatch b, short limit) {
            short rows = 0;
            boolean hasUnset = (statePlain()&HAS_UNSET_OUT) != 0;
            byte outCol0 = this.outCol0, outCol1 = this.outCol1;
            long[] a = b.arr;
            long deadline = Timestamp.nextTick(LIMIT_TICKS);
            var it = (Triples.PairIt) this.it;
            for (int base = 0; rows < limit && (retry = it.advance()); base += cols) {
                if (hasUnset      ) arraycopy(rowSkels, rowSkelBegin, a, base, cols);
                if (outCol0   >= 0) a[base+outCol0] = source(it.subKeyId, dictId);
                if (outCol1   >= 0) a[base+outCol1] = source(it.valueId,  dictId);
                ++rows;
                if ((rows&DEADLINE_CHK) == DEADLINE_CHK && Timestamp.nanoTime() > deadline) break;
            }
            b.rows = rows;
        }

        private void fillSubKey(StoreBatch b, short limit) {
            short rows = 0;
            byte outCol0 = this.outCol0;
            boolean hasUnset = (statePlain()&HAS_UNSET_OUT) != 0;
            long[] a = b.arr;
            long deadline = Timestamp.nextTick(LIMIT_TICKS);
            var it = (Triples.SubKeyIt)this.it;
            for (int base = 0; rows < limit && (retry = it.advance()); base += cols) {
                if (hasUnset      ) arraycopy(rowSkels, rowSkelBegin, a, base, cols);
                if (outCol0   >= 0) a[base+outCol0] = source(it.subKeyId, dictId);
                ++rows;
                if ((rows&DEADLINE_CHK) == DEADLINE_CHK && Timestamp.nanoTime() > deadline) break;
            }
            b.rows = rows;
        }

        private void fillScan(StoreBatch b, short limit) {
            short rows = 0;
            boolean hasUnset = (statePlain()&HAS_UNSET_OUT) != 0;
            byte outCol0 = this.outCol0, outCol1 = this.outCol1, outCol2 = this.outCol2;
            long[] a = b.arr;
            long deadline = Timestamp.nextTick(LIMIT_TICKS);
            var it = (Triples.ScanIt) this.it;
            for (int base = 0; rows < limit && (retry = it.advance()); base += cols) {
                if (hasUnset      ) arraycopy(rowSkels, rowSkelBegin, a, base, cols);
                if (outCol0   >= 0) a[base+outCol0] = source(it.keyId,    dictId);
                if (outCol1   >= 0) a[base+outCol1] = source(it.subKeyId, dictId);
                if (outCol2   >= 0) a[base+outCol2] = source(it.valueId,  dictId);
                ++rows;
                if ((rows&DEADLINE_CHK) == DEADLINE_CHK && Timestamp.nanoTime() > deadline) break;
            }
            b.rows = rows;
        }
    }

    public static boolean ALT = true;

    private <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    converterFromStore(BatchType<B> bt, Orphan<? extends Emitter<StoreBatch, ?>> upstream) {
        var conv =  new FromStoreConverter<>(bt, upstream);
        if (ALT && hugeDict && conv.cols() > 0)
            return AsyncStage.create(conv);
        return conv;
    }

    private final class FromStoreConverter<B extends Batch<B>>
            extends ConverterStage<StoreBatch, B, FromStoreConverter<B>>
            implements Orphan<FromStoreConverter<B>> {
        private final LocalityCompositeDict.Lookup lookup;
        private int avgRowLocalLen;

        public FromStoreConverter(BatchType<B> type,
                                  Orphan<? extends Emitter<StoreBatch, ?>> upstream) {
            super(type, upstream);
            this.lookup = dict.lookup().takeOwnership(this);
            avgRowLocalLen = 12*this.upstream.vars().size();
        }

        @Override public FromStoreConverter<B> takeOwnership(Object o) {return takeOwnership0(o);}

        @Override protected void doRelease() {
            Owned.safeRecycle(lookup, this);
            super.doRelease();
        }

        @Override public void onBatchByCopy(StoreBatch batch) {
            if (EmitterStats.ENABLED && stats != null)
                stats.onBatchPassThrough(batch);
            B dst = batchType.createForThread(threadId, cols).takeOwnership(this);
            short rows, rll = (short)avgRowLocalLen;
            for (var b = batch; b != null; b = b.next) {
                dst.reserveAddLocals((rows=b.rows) * rll);
                long[] ids = b.arr;
                for (int r = 0; r < rows; ++r)
                    putRowConverting(dst, ids, cols, r*cols);
            }
            avgRowLocalLen = (7*rll + dst.avgLocalBytesUsed())>>3;
            downstream.onBatch(dst.releaseOwnership(this));
        }

        private short cols() { return cols; }

        private FinalSegmentRope shared(TwoSegmentRope t, byte fst) {
            return switch (fst) {
                case '"' -> SHARED_ROPES.internDatatypeOf(t, 0, t.len);
                case '<' -> {
                    if (t.fstLen == 0 || t.sndLen == 0) yield FinalSegmentRope.EMPTY;
                    int slot = (int)(t.fstOff & PREFIXES_MASK);
                    var cached = prefixes[slot];
                    if (cached == null || cached.offset != t.fstOff || cached.segment != t.fst) {
                        cached = new FinalSegmentRope(t.fst, t.fstU8, t.fstOff, t.fstLen);
                        prefixes[slot] = cached;
                    }
                    yield cached;
                }
                case '_' -> FinalSegmentRope.EMPTY;
                default -> throw new IllegalArgumentException("Not an RDF term");
            };
        }

        private void putRowConverting(B dest, long[] ids, int cols, int base) {
            dest.beginPut();
            for (int c = 0; c < cols; c++) {
                var t = lookup.get(unsource(ids[base + c]));
                if (t == null) continue;
                byte fst = t.get(0);
                var sh = shared(t, fst);
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

    private final class StoreBindingEmitter<B extends Batch<B>>
            extends BindingStage<B, StoreBindingEmitter<B>>
            implements Orphan<StoreBindingEmitter<B>> {
        private static final LocalityCompositeDict.LocalityLexIt [] EMPTY_LEX_ITS
                = new LocalityCompositeDict.LocalityLexIt[0];
        /** The right operand algebra used to create this emitter. Immutable */
        private final Plan rightPlan;
        /** Mutable copy of {@link Modifier#filters} if {@code rightPlan} is a {@link Modifier}.*/
        private final List<Expr> tmpRightFilters;
        /** If a lexical join hold lexical variants of the left-provided binding using the
         *  var names of the right operand instead of the left-side binding var name. */
        private @MonotonicNonNull BatchBinding lexJoinBinding;
        /** Next {@code lexJoinBinding} with its own captive {@link Batch}, continuously
         * swapped with {@code lexJoinBinding} */
        private @MonotonicNonNull BatchBinding nextLexJoinBinding;
        /** The i-th lexIt iterates over variants of the {@code lexItCols[i]}-th binding var */
        private int @MonotonicNonNull[] lexItsCols;
        /** array of non-null lexical variant iterators or null if there is no lexical join */
        private LocalityCompositeDict.LocalityLexIt  @MonotonicNonNull[] lexIts;
        /** used to convert ids from {@code lexIts} into values for {@code nextLexJoinBatch}. */
        private LocalityCompositeDict.@MonotonicNonNull Lookup lookup;
        /** shortcut bitset for checking if a column of {@code nextLexJoinBatch} should be
         * filled with a value from a {@code lexIt} instead of just copied from
         * {@code lexJoinBinding.batch}*/
        private long lexBindingCols;
        private final short leftCols;
        private @MonotonicNonNull TwoSegmentRope lexView;
        private @MonotonicNonNull SegmentRopeView localView;
        /** {@code bb} from last {@code rebind(bb)}, but all batches are instances of {@code B} */
        private @MonotonicNonNull BatchBinding convIntBinding;
        /** batches used in {@code convIntBinding} and owned by {@code this} */
        private @MonotonicNonNull ArrayList<B> convBindingBatches;

        public StoreBindingEmitter(EmitBindQuery<B> bq, Plan right,
                                   Vars rebindHint) {
            super(bq.bindings, bq.type, bq, bq.resultVars(),
                  emit(bq.batchType(), right,
                       bq.bindingsVars().union(rebindHint)));
            int bindingsVarsCount = bq.bindingsVars().size();
            if (bindingsVarsCount > Short.MAX_VALUE)
                throw new IllegalArgumentException("Too many binding vars");
            this.leftCols  = (short)bindingsVarsCount;
            rightPlan      = right;
            lexIts         = EMPTY_LEX_ITS;
            lexItsCols     = ArrayAlloc.EMPTY_INT;
            if (right instanceof Modifier m && !m.filters.isEmpty()) {
                tmpRightFilters = new ArrayList<>(m.filters);
                updateExtRebindVars(BatchBinding.ofEmpty(batchType()));
            } else {
                tmpRightFilters = null;
            }
        }


        @SuppressWarnings("unchecked") @Override protected void doRelease() {
            super.doRelease();
            B lexBatch = null, nextLexBatch = null;
            if (lexJoinBinding != null) {
                lexBatch = (B)lexJoinBinding.batch;
                nextLexBatch = (B)requireNonNull(nextLexJoinBinding).batch;
                lexJoinBinding.attach(Batch.safeRecycle(lexBatch, this), 0);
            }
            if (nextLexBatch != lexBatch)
                Batch.safeRecycle(nextLexBatch, this);
            if (convBindingBatches != null) {
                for (int i = 0, n = convBindingBatches.size(); i < n; i++)
                    convBindingBatches.set(i, Batch.safeRecycle(convBindingBatches.get(i), this));
                // detach recycled batches from binding, in case is is reachable elsewhere
                for (var bb = convIntBinding; bb != null; bb = bb.remainder)
                    bb.attach(null, 0);
            }
            lookup = Owned.safeRecycle(lookup, this);
        }

        @Override public StoreBindingEmitter<B> takeOwnership(Object o) {return takeOwnership0(o);}

        private BatchBinding createLexJoinBinding(BatchBinding lexJoinBinding,
                                                  Plan right, List<Expr> mFilters, Vars leftVars) {
            Vars rightBindingVars = null;
            try (var term = PooledTermView.ofEmptyString();
                 var varRope = PooledMutableRope.getWithCapacity(16)) {
                for (SegmentRope name : right.allVars()) {
                    varRope.len = 1;
                    varRope.append(name).u8()[0] = '?';
                    term.wrap(FinalSegmentRope.EMPTY, varRope, false);
                    Term leftVar = leftLexJoinVar(mFilters, term, leftVars);
                    if (leftVar != term) {
                        if (rightBindingVars == null)
                            rightBindingVars = Vars.fromSet(leftVars, leftVars.size());
                        int i = rightBindingVars.indexOf(leftVar);
                        if (i >= 0)
                            rightBindingVars.set(i, name);
                    }
                }
            }
            if (rightBindingVars != null) {
                if (lexJoinBinding == null)
                    lexJoinBinding = new BatchBinding(rightBindingVars);
                else
                    lexJoinBinding.vars(rightBindingVars);
                //noinspection unchecked
                B b = batchType.empty((B)lexJoinBinding.batch, this, rightBindingVars.size());
                b.beginPut();
                b.commitPut();
                lexJoinBinding.attach(b, 0);
                return lexJoinBinding;
            }
            return null;
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            if (binding.sequence == lastRebindSeq)
                return; // duplicate call due to diamond in emitter graph
            super.rebind(binding);
            if (lexJoinBinding != null)  // updated by super.rebind()->updateExtRebindVars()
                lexRebindExternal(binding);
        }

        @SuppressWarnings("DataFlowIssue")
        private void lexRebindExternal(BatchBinding binding) {
            BatchBinding conv = convIntBinding.remainder;
            if (intBindingVars().size() == leftCols) {
                // do not copy-convert external binding if its values will not be used
                for (var bb = binding; bb != null; bb = bb.remainder) {
                    conv.attach(bb.batch, bb.row);
                    if (bb.remainder != null) {
                        if (conv.remainder == null)
                            conv.remainder = new BatchBinding(bb.remainder.vars);
                        conv = conv.remainder;
                    }
                }
            } else {
                int i = 1;
                for (BatchBinding bb = binding; bb != null; bb = bb.remainder) {
                    convertBindingNode(conv, bb, i++);
                    if (bb.remainder != null) {
                        if (conv.remainder == null)
                            conv.remainder = new BatchBinding(bb.remainder.vars);
                        conv = conv.remainder;
                    }
                }
            }
        }

        private void convertBindingNode(BatchBinding dst, BatchBinding src, int i) {
            Batch<?> b = src.batch;
            B cb;
            short cr;
            if (b == null || b.type().equals(batchType)) {
                cr = src.row;
                //noinspection unchecked
                cb = (B)b;
            } else {
                cr = 0;
                cb = batchType.empty(convBindingBatches.get(i), this, b.cols);
                convBindingBatches.set(i, cb);
                if (cb instanceof StoreBatch sb) sb.putRowConverting(b, src.row, dictId);
                else                             cb.putRowConverting(b, src.row);
            }
            dst.attach(cb, cr);
        }

        @Override protected void updateExtRebindVars(BatchBinding binding) {
            super.updateExtRebindVars(binding);
            if (tmpRightFilters == null || !(rightPlan instanceof Modifier m))
                return;
            tmpRightFilters.clear();
            tmpRightFilters.addAll(m.filters);
            Vars union = intBindingVars();
            lexJoinBinding = createLexJoinBinding(lexJoinBinding, m, tmpRightFilters, union);
            if (lexJoinBinding != null) {
                // setup lexIts/lexItsCols
                Vars rightBindingVars = lexJoinBinding.vars;
                int nVars = union.size();
                assert rightBindingVars.size() == nVars;
                int lexJoins = 0;
                for (int i = 0; i < nVars; i++) {
                    if (union.get(i) != rightBindingVars.get(i)) lexJoins++;
                }
                if (ENABLED)
                    journal("enabling lexical join of ", lexJoins, "columns on", this);
                lexItsCols = new int[lexJoins];
                lexIts = new LocalityCompositeDict.LocalityLexIt[lexJoins];
                long lexBindingCols = 0;
                lexJoins = 0;
                for (int i = 0; i < nVars; i++) {
                    SegmentRope extVar = union.get(i), intVar = rightBindingVars.get(i);
                    if (extVar == intVar) continue;
                    if (ENABLED) journal("lexical join of ", extVar, "with", intVar);
                    if (i  < 64) lexBindingCols |= 1L << i;
                    lexItsCols[lexJoins]   = i;
                    lexIts    [lexJoins++] = dict.lexIt();
                }
                this.lexBindingCols = lexBindingCols;

                // update filters on upstream
                for (Emitter<?, ?> em = rightUpstream(); em != null; ) {
                    if (em instanceof BatchFilter<?, ?> bf) {
                        em = null;
                        if (bf.rowFilter instanceof Modifier.Filtering<?, ?> f)
                            f.setFilters(tmpRightFilters);
                        else if ((em = bf.before) == null)
                            em = bf.upstream();
                    } else if (em instanceof Stage<?,?,?> s) {
                        em = s.upstream();
                    } else {
                        throw new IllegalArgumentException("No Filtering found in right upstream");
                    }
                }

                // lazy initializations
                if (nextLexJoinBinding == null)
                    nextLexJoinBinding = new BatchBinding(lexJoinBinding.vars);
                //noinspection unchecked
                nextLexJoinBinding.batch = batchType.empty((B)nextLexJoinBinding.batch, this, nVars);
                if (lookup == null)
                    lookup = dict.lookup().takeOwnership(this);
                if (lexView == null)
                    lexView = new TwoSegmentRope();
                if (localView == null)
                    localView = new SegmentRopeView();
                if (convIntBinding == null) {
                    convBindingBatches = new ArrayList<>();
                    convIntBinding = new BatchBinding(union);
                    convIntBinding.remainder = new BatchBinding(binding.vars);
                } else {
                    convIntBinding.vars(union);
                    //noinspection DataFlowIssue
                    convIntBinding.remainder.vars(binding.vars);
                }
            }
        }

        @Override protected void rebind(BatchBinding binding, Emitter<B, ?> rightEmitter) {
            if (lexJoinBinding != null)
                binding = lexRebindInternal(binding);
            super.rebind(binding, rightEmitter);
        }

        private BatchBinding lexRebindInternal(BatchBinding binding) {
            var convBinding = convIntBinding;
            convertBindingNode(convBinding, binding, 0);

            @SuppressWarnings("unchecked") B lexBatch = (B)nextLexJoinBinding.batch;
            assert lexBatch != null && lexView != null && lookup != null;
            clearAndCopyNonLexBindings(convBinding, lexBatch);
            if (binding.vars.size() > 64)
                copyNonLexBindingsExtra(convBinding, lexBatch);
            for (int i = 0; i < lexIts.length; i++) {
                if (convBinding.get(lexItsCols[i], lexView)) {
                    var it = lexIts[i];
                    it.find(lexView);
                    if (it.advance())
                        putLex(lexBatch, i, lookup);
                    else
                        convBinding.putTerm(lexItsCols[i], lexBatch, lexItsCols[i]);
                }
            }
            lexBatch.commitPut();
            return swapLexJoinBinding();
        }

        private BatchBinding swapLexJoinBinding() {
            BatchBinding b     = nextLexJoinBinding;
            b.sequence         = ++lexJoinBinding.sequence;
            nextLexJoinBinding = lexJoinBinding;
            lexJoinBinding     = b;
            return b;
        }

        private void clearAndCopyNonLexBindings(BatchBinding src, B dst) {
            dst.clear();
            dst.beginPut();
            for (int c = 0, n = min(64, dst.cols); c < n; c++) {
                if ((lexBindingCols & (1L << c)) == 0)
                    src.putTerm(c, dst, c);
            }
        }

        private void copyNonLexBindingsExtra(BatchBinding src, B dst) {
            outer:
            for (int c = 64, n = src.cols; c < n; c++) {
                for (int lexCol : lexItsCols) {
                    if (lexCol == c) continue outer;
                }
                src.putTerm(c, dst, c);
            }
        }

        private void putLex(B nextLexBatch, int lexIdx, LocalityCompositeDict.Lookup lookup) {
            long id = lexIts[lexIdx].id;
            int dstCol = lexItsCols[lexIdx];
            SegmentRope local;
            if (nextLexBatch instanceof StoreBatch sb) {
                sb.putTerm(dstCol, source(id, dictId));
            } else if ((local = lookup.getLocal(id)) != null) {
                nextLexBatch.putTerm(dstCol, FinalSegmentRope.asFinal(lookup.getShared(id)), local,
                                     0, local.len, lookup.sharedSuffixed(id));
            }
        }

        @Override protected boolean lexContinueRight(BatchBinding binding,
                                                     Emitter<B, ?> rightEmitter) {
            BatchBinding lexJoinBinding = this.lexJoinBinding;
            if (lexJoinBinding == null)
                return false;
            int nIts = lexIts.length, i = nIts-1;
            while (i >= 0 && !lexIts[i].advance())
                --i;
            if (i < 0)
                return false;

            //noinspection unchecked
            B lexBatch = (B)lexJoinBinding.batch, nextLexBatch = (B)nextLexJoinBinding.batch;
            assert lexBatch != null && nextLexBatch != null;
            int nVars = lexBatch.cols;

            clearAndCopyNonLexBindings(lexJoinBinding, nextLexBatch);
            if (nVars > 64)
                copyNonLexBindingsExtra(lexJoinBinding, nextLexBatch);
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
            rightEmitter.rebind(swapLexJoinBinding());
            return true;
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
        private final TriplePattern tp;

        public StoreIteratorBIt(Vars outVars, int term0Col, int term1Col, TriplePattern tp) {
            super(TYPE, outVars);
            this.dictId = (short) StoreSparqlClient.this.dictId;
            if (term0Col > 127 || term1Col > 127)
                throw new IllegalArgumentException("too many columns");
            this.term0Col = (byte)term0Col;
            this.term1Col = (byte)term1Col;
            this.tp = tp;
            acquireRef();
        }

        @Override protected void appendToSimpleLabel(StringBuilder sb) {
            StoreSparqlClient.appendToSimpleLabel(sb, endpoint, tp);
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            try {
                super.cleanup(cause);
            } finally { releaseRef(); }
        }
    }

    final class StoreValueBIt extends StoreIteratorBIt {
        private final Triples.ValueIt vit;

        public StoreValueBIt(Vars vars, TriplePattern tp, Term valueVar, Triples.ValueIt vit) {
            super(vars, vars.indexOf(valueVar), -1, tp);
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

        public StorePairBIt(Vars pubVars, TriplePattern tp, Term subKeyVar, Term valueVar, Triples.PairIt pit) {
            super(pubVars, pubVars.indexOf(subKeyVar), pubVars.indexOf(valueVar), tp);
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

        public StoreSubKeyBIt(Vars pubVars, TriplePattern tp, Term subKeyVar, Triples.SubKeyIt sit) {
            super(pubVars, pubVars.indexOf(subKeyVar), -1, tp);
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
        private final TriplePattern tp;

        public StoreScanIt(Vars vars, TriplePattern tp) {
            super(TYPE, vars);
            int sCol = vars.indexOf(tp.s), pCol = vars.indexOf(tp.p), oCol = vars.indexOf(tp.o);
            if (sCol > 127 || pCol > 127 || oCol > 127)
                throw new IllegalArgumentException("Too many vars");
            this.dictId = (short) StoreSparqlClient.this.dictId;
            this.sCol = (byte)sCol;
            this.pCol = (byte)pCol;
            this.oCol = (byte)oCol;
            this.sit = spo.scan();
            this.tp = tp;
            acquireRef();
        }

        @Override protected void appendToSimpleLabel(StringBuilder sb) {
            StoreSparqlClient.appendToSimpleLabel(sb, endpoint, tp);
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
        private final @Nullable BatchMerger<B, ?> merger;
        private final TripleRoleSet tpFreeRoles;
        private final long s, p, o;
        private long sNotLex, pNotLex, oNotLex;
        private final byte sLeftCol, pLeftCol, oLeftCol;
        private final byte kCol, skCol, vCol, tpFreeCols;
        private final short dictId;
        private boolean rEnd, rEmpty;
        private final boolean rightSingleRow;
        private final TwoSegmentRope ropeView;
        private final @Nullable BatchMerger<B, ?> preFilterMerger;
        private final @Nullable BatchFilter<B, ?> rightFilter;
        private final boolean hasLexicalJoin;
        private final LocalityCompositeDict.@Nullable LocalityLexIt sLexIt, pLexIt, oLexIt;
        private final TriplePattern tp;

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
            this.tp = tp;
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
            this.rb = batchType.create(tpFreeCols).takeOwnership(this);

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
                    || m.distinct != null)) {
                boolean needsLeftVars = false;
                for (Expr expr : mFilters) {
                    if (!tpFree.containsAll(expr.vars())) { needsLeftVars = true; break; }
                }
                if (needsLeftVars) {
                    procRightFree = leftVars.union(tpFree);
                    preFilterMerger = batchType.merger(procRightFree, leftVars, tpFree).takeOwnership(this);
                    fb = batchType.create(procRightFree.size()).takeOwnership(this);
                } else {
                    procRightFree = tpFree;
                    preFilterMerger = null;
                }
                var proc = m.processorFor(batchType, procRightFree, null, m.distinct);
                rightFilter = (BatchFilter<B, ?>)proc.takeOwnership(this);
            } else {
                procRightFree = tpFree;
                m = null;
                preFilterMerger = null;
                rightFilter = null;
            }
            if (procRightFree.size() > Short.MAX_VALUE)
                throw new IllegalArgumentException("Too many (>32767) left+right columns");
            this.rightSingleRow = switch (bindType) {
                case JOIN, LEFT_JOIN           -> m != null && m.limit == 1;
                case EXISTS, NOT_EXISTS, MINUS -> true;
            };
            this.rEnd = true;
            this.rEmpty = true;
            this.merger = !bindType.isJoin() || procRightFree.equals(projection) ? null
                    : batchType.merger(projection, leftVars, procRightFree).takeOwnership(this);
            this.lb = batchType.create(leftVars.size()).takeOwnership(this);
            acquireRef();
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return Stream.of(left);
        }

        @Override protected void appendToSimpleLabel(StringBuilder sb) {
            StoreSparqlClient.appendToSimpleLabel(sb, endpoint, tp);
        }

        @Override public @This BIt<B> eager() {
            left.eager();
            return super.eager();
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            try {
                // close() and failures from nextBatch() are rare. Rarely generating garbage is
                // cheaper than precisely tracking whether lb ownership is with this or with left,
                if (cause == null) {
                    lb = Batch.safeRecycle(lb, this); // signals exhaustion to nextBatch()
                }
                // if we arrived here from close(), nextBatch() may be concurrently executing.
                // it is cheaper to leak rb and fb than to synchronize
                if (!(cause instanceof BItCancelledException)) {
                    rb = Batch.safeRecycle(rb, this);
                    fb = Batch.safeRecycle(fb, this);
                }
                Owned.safeRecycle(rightFilter, this);
            } finally {
                try {
                    super.cleanup(cause);
                } finally {
                    releaseRef();
                }
            }
        }

        @Override public boolean tryCancel() {
            boolean did = super.tryCancel();
            if (did)
                left.tryCancel();
            return did;
        }

        @Override public @Nullable Orphan<B> nextBatch(@Nullable Orphan<B> orphan) {
            if (lb == null) return null; // already exhausted
            boolean locked = false;
            try {
                long startNs = needsStartTime ? Timestamp.nanoTime() : Timestamp.ORIGIN;
                long innerDeadline = rightSingleRow ? Timestamp.ORIGIN-1 : startNs+minWaitNs;
                B b = orphan == null ? batchType.create(nColumns).takeOwnership(this)
                                     : orphan.takeOwnership(this).clear(nColumns);
                do {
                    lock();
                    locked = true;
                    if (plainState.isTerminated() || (rEnd && !rebind()))
                        break; // cancelled or reached end of bindings
                    // fill rb with values from bound rIt
                    rb = rb.clear(tpFreeCols);
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
                        if (preFilterMerger != null) {
                            var dst = Owned.releaseOwnership(fb, this);
                            fb = null;
                            prb = fb = preFilterMerger.merge(dst, lb, lr, rb).takeOwnership(this);
                        }
                        var prbOrphan = prb.releaseOwnership(this);
                        prb = takeOwnership(rightFilter.filterInPlace(prbOrphan), this);
                        if (preFilterMerger == null)
                            rb = prb; // filterInPlace() may have recycled rb
                        if (prb.rows == 0) {
                            unlock();
                            locked = false;
                            continue;
                        }
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
                                    b = merger.merge(b.releaseOwnership(this), lb, lr, prb)
                                              .takeOwnership(this);
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
                    unlock();
                    locked = false;
                } while (readyInNanos(b.totalRows(), startNs) > 0);
                if (!locked) {
                    lock();
                    locked = true;
                }
                if (bindingNotifier != null && bindingNotifier.bindQuery.metrics != null)
                    bindingNotifier.bindQuery.metrics.batch(b.totalRows());
                return b.rows == 0 ? handleEmptyBatch(b) : onNextBatch(b.releaseOwnership(this));
            } catch (Throwable t) {
                if (state() == State.ACTIVE)
                    onTermination(t);
                lb = null; // signal exhaustion
                throw t;
            } finally {
                if (locked)
                    unlock();
            }
        }

        private Orphan<B> handleEmptyBatch(B batch) {
            batch.recycle(this);
            onTermination(null);
            return null;
        }

        private void putRow(long kId, long skId, long vId) {
            rb.beginPut();
            if (rb instanceof StoreBatch sb) {
                StoreBatch tail = sb.tail();
                if ( kCol >= 0) tail.doPutTerm(kCol,  source( kId, dictId));
                if (skCol >= 0) tail.doPutTerm(skCol, source(skId, dictId));
                if ( vCol >= 0) tail.doPutTerm(vCol,  source( vId, dictId));
            } else {
                var lookup = dict.lookup().takeOwnership(this);
                try {
                    if ( kCol >= 0) convertTerm(rb, kCol,   kId, lookup);
                    if (skCol >= 0) convertTerm(rb, skCol, skId, lookup);
                    if ( vCol >= 0) convertTerm(rb, vCol,   vId, lookup);
                } finally {
                    lookup.recycle(this);
                }
            }
            rb.commitPut();
        }

        private void convertTerm(B dest, byte col, long id, LocalityCompositeDict.Lookup lookup) {
            TwoSegmentRope t = lookup.get(id);
            if (t == null) return;
            byte fst = t.get(0);
            FinalSegmentRope sh = switch (fst) {
                case '"' -> SHARED_ROPES.internDatatypeOf(t, 0, t.len);
                case '<' -> {
                    if (t.fstLen == 0 || t.sndLen == 0) yield FinalSegmentRope.EMPTY;
                    int slot = (int)(t.fstOff & PREFIXES_MASK);
                    var cached = prefixes[slot];
                    if (cached == null || cached.offset != t.fstOff || cached.segment != t.fst) {
                        cached = new FinalSegmentRope(t.fst, t.fstU8, t.fstOff, t.fstLen);
                        prefixes[slot] = cached;
                    }
                    yield cached;
                }
                case '_' -> FinalSegmentRope.EMPTY;
                default -> throw new IllegalArgumentException("Not an RDF term");
            };
            int localOff = fst == '<' ? sh.len : 0;
            dest.putTerm(col, sh, t, localOff, t.len-sh.len, fst == '"');
        }

        private boolean resetLexIt(LexIt<?> lexIt, byte leftCol) {
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

        private long lexRebindCol(LexIt<?> lexIt, byte leftCol, long fallback,
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
                    B n = lb.dropHead(this);
                    if (n != null) {
                        lb = n;
                    } else {
                        lb = Orphan.takeOwnership(left.nextBatch(null), this);
                        if (startBindingNotifier != null) startBindingNotifier.startBinding();
                        if (lb == null) return false;
                    }
                }
                rEmpty = true;
                var l = dict.lookup().takeOwnership(this);
                try {
                    sNotLex = s = lexRebindCol(sLexIt, sLeftCol, this.s, lb, lr, l);
                    pNotLex = p = lexRebindCol(pLexIt, pLeftCol, this.p, lb, lr, l);
                    oNotLex = o = lexRebindCol(oLexIt, oLeftCol, this.o, lb, lr, l);
                } finally {
                    l.recycle(this);
                }
            }
            rebind(s, p, o);
            return true;
        }

        private boolean rebind() {
            if (hasLexicalJoin)
                return rebindLexical();
            if (++lr >= lb.rows) {
                lr = 0;
                B n = lb.dropHead(this);
                if (n != null) {
                    lb = n;
                } else {
                    if (eager)
                        left.tempEager();
                    B nextLB = null;
                    unlock();
                    try { //
                        nextLB = Orphan.takeOwnership(left.nextBatch(null), this);
                    } finally {
                        lock();
                        if (isTerminated()) // tryCancel()/close() while unlocked
                            nextLB = Batch.safeRecycle(nextLB, this); // abort rebind
                    }
                    if ((lb = nextLB) == null)
                        return false;
                }
            }
            if (startBindingNotifier != null) startBindingNotifier.startBinding();
            rEmpty = true;
            B lb = this.lb;
            int lr = this.lr;
            long s, p, o;
            var l = dict.lookup().takeOwnership(this);
            try {
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
            } finally {
                l.recycle(this);
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
