package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.UnboundSparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.fed.Selector.InitOrigin;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsListener;
import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.util.BS;
import com.github.alexishuf.fastersparql.util.ExceptionCondenser;
import com.github.alexishuf.fastersparql.util.IOUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

import static com.github.alexishuf.fastersparql.FS.project;
import static com.github.alexishuf.fastersparql.FSProperties.dedupCapacity;
import static com.github.alexishuf.fastersparql.util.BS.*;
import static com.github.alexishuf.fastersparql.util.ExceptionCondenser.closeAll;
import static com.github.alexishuf.fastersparql.util.ExceptionCondenser.runtimeExceptionCondenser;
import static java.lang.Long.bitCount;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.System.nanoTime;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("unused")
public class Federation extends AbstractSparqlClient {
    private static final String SOURCES = "sources";

    private final List<Source> sources = new ArrayList<>();
    private final Optimizer optimizer = new Optimizer();
    private final List<FedMetricsListener> fedListeners = new ArrayList<>();
    private final List<MetricsListener> planListeners = new ArrayList<>();
    private final Path specPathsRelativeTo;
    private final Lock lock = new ReentrantLock();
    private int cdc = FSProperties.dedupCapacity();
    private boolean closed;
    private CompletableFuture<Void> init;

    public Federation(SparqlEndpoint endpoint,
                      @Nullable Path specPathsRelativeTo) {
        super(endpoint);
        (init = new CompletableFuture<>()).complete(null);
        this.specPathsRelativeTo = specPathsRelativeTo;
    }

    /* --- --- --- load/save --- --- --- */

    public static  Federation load(File tomlFile) throws IOException {
        return load(tomlFile.toPath());
    }
    public static  Federation load(Path tomlFile) throws IOException {
        return load(Spec.parseToml(tomlFile));
    }
    public static  Federation load(Spec spec) throws IOException {
        var sources = new ArrayList<Source>();
        for (Spec sourceSpec : spec.getListOf(SOURCES, Spec.class))
            sources.add(Source.load(sourceSpec));
        SparqlEndpoint endpoint = SparqlEndpoint.parse(Source.readUrl(spec));
        Path relativeTo = spec.get(Spec.PATHS_RELATIVE_TO, Path.class);
        Federation fed = new Federation(endpoint, relativeTo);
        fed.addSources(sources);
        return fed;
    }

    /**
     * Saves the specification of the federation to the given TOML file and saves
     * {@link Selector} states to the paths set in the configuration of the sources.
     *
     * <p>If this Federation was loaded from a spec file that had an absolute path set as
     * {@link Spec#PATHS_RELATIVE_TO}, that same path will be used to resolve relative
     * paths used to specify state files for {@link Selector}s. If said path was not defined,
     * the parent dir of {@code destinationFile} will be used to resolve relative paths. If it was
     * defined as a relative path, it will be resolved against {@code destinationFile}'s parent
     * dir and the result will then be used to resolve local paths.</p>
     *
     * @param destinationFile destination TOML file for the federation spec file.
     */
    public CompletionStage<Void> save(File destinationFile)  {
        Path parent = destinationFile.toPath().getParent();
        if (parent == null) parent = new File("").toPath();

        Path relToAbsolute = specPathsRelativeTo;
        if (relToAbsolute == null) relToAbsolute = parent;
        else if (!relToAbsolute.isAbsolute()) relToAbsolute = parent.resolve(relToAbsolute);

        Spec root = new Spec(new HashMap<>());
        root.set(Source.URL, endpoint.uri());

        List<Spec> sourceSpecs;
        CompletionStage<Void> statesStage;
        lock.lock();
        try {
            sourceSpecs = new ArrayList<>(sources.size());
            for (Source s : sources)
                sourceSpecs.add(s.spec());

            statesStage = saveIfEnabled(relToAbsolute);
        } finally {
            lock.unlock();
        }
        root.set(SOURCES, sourceSpecs);

        var errs = runtimeExceptionCondenser();
        try {
            IOUtils.writeWithTmp(destinationFile, os -> {
                try (var w = new OutputStreamWriter(os, UTF_8)) {
                    root.toToml(w);
                }
            });
        } catch (IOException e) {
            errs.condense(e);
        }
        return errs.condense(null, statesStage);
    }

    /* --- --- --- sources and state management --- --- --- */

    /**
     * Calls {@link Selector#saveIfEnabled(Path)} for all sources and return a
     *
     * @param pathsRelativeTo see {@link Selector#saveIfEnabled(Path)}.
     * @return {@link CompletionStage} with either a {@link Throwable} or the return from
     *         {@link Selector#saveIfEnabled(Path)} for each {@link Source}. The
     *         {@link CompletionStage} itself never fails, since failures are individual
     *         to each source.
     */
    public CompletionStage<Void> saveIfEnabled(@Nullable Path pathsRelativeTo) {
        return this.forEachSource((source, h) ->
                source.selector().saveIfEnabled(pathsRelativeTo).handle(h));
    }

    /**
     * Get a {@link CompletionStage} that will complete once all sources have been initialized.
     *
     * @return {@link CompletionStage} with either a {@link Throwable} or the result from
     *         {@link Selector#initialization()} for each {@link Source}. The
     *         {@link CompletionStage} itself never fails, since failures are individual
     *         to each source.
     * @see Selector#initialization()
     */
    public CompletionStage<Void> init() {  return init; }

    interface SourceTaskHandler<V> extends BiFunction<V, Throwable, Void> {}

    @FunctionalInterface
    interface SourceTaskLauncher<V> {
        void launch(Source source, SourceTaskHandler<V> callback);
    }

    private <V> CompletionStage<Void>
    forEachSource(SourceTaskLauncher<V> launcher) {
        var future = new CompletableFuture<Void>();
        var err = new ExceptionCondenser<>(RuntimeException.class, RuntimeException::new);
        lock.lock();
        try {
            if (closed)
                throw new IllegalStateException("Federation closed");
            AtomicInteger pending = new AtomicInteger(sources.size());
            for (Source source : sources) {
                launcher.launch(source, (ignored, throwable) -> {
                    if (throwable != null) err.condense(throwable);
                    if (pending.decrementAndGet() == 0)
                        err.complete(future, null);
                    return null;
                });
            }
        } finally { lock.unlock(); }
        return future;
    }

    /** Add all sources in {@code collection} to this {@link Federation}. */
    public void addSources(Collection<Source> collection) {
        lock.lock();
        try {
            if (closed)
                throw new IllegalStateException("Federation closed");
            var initialized = this.init = new CompletableFuture<>();
            sources.addAll(collection);
            for (Source s : collection)
                optimizer.estimator(s.client, s.estimator);
            this.<InitOrigin>forEachSource(
                    (s, handler) -> s.selector().initialization().handle(handler)
            ).thenAccept(initialized::complete);
        } finally {
            lock.unlock();
        }
    }

    /** Add a source to this federation. Prefer using {@link Federation#addSources(Collection)}. */
    public void addSource(Source source) { addSources(List.of(source)); }

    /** Adds a listener to receive {@link FedMetrics} objects for each dispatched query */
    public void addFedListener(FedMetricsListener fedListener) {
        lock.lock();
        try {
            fedListeners.add(fedListener);
        } finally { lock.unlock(); }
    }

    /** Adds a listener to receive {@link Metrics} for each {@link Plan} executed to answer
     *  a federated query. */
    public void addPlanListener(MetricsListener planListener) {
        lock.lock();
        try {
            planListeners.add(planListener);
        } finally { lock.unlock(); }
    }

    @Override public void close() {
        lock.lock();
        try {
            if (closed) return;
            closed = true;
        } finally {
            lock.unlock();
        }
        closeAll(RuntimeException.class, RuntimeException::new, sources);
    }

    /* --- --- --- querying --- --- --- */

    @Override public <R> BIt<R> query(RowType<R> rowType, SparqlQuery sparql) {
        long entryNs = nanoTime();
        cdc = FSProperties.dedupCapacity();

        // parse query or copy tree
        var m = new FedMetrics(this, sparql);
        Plan root = project(switch (sparql) {
            case OpaqueSparqlQuery o -> {
                Plan plan = new SparqlParser().parse(o.sparql);
                yield mutateSanitize(plan);
            }
            case Plan p -> p.transform(sanitizeTransformer, null);
            default -> throw new IllegalArgumentException("Unexpected SparqlQuery implementation");
        }, sparql.publicVars());

        // source selection & agglutination
        long last = nanoTime();
        root = sources.size() < 2 ? trivialPlan(root, m) : plan(root, m);
        last = m.addSelectionAndAgglutination(last);

        // optimization
        root = optimizer.optimize(root);
        m.addOptimization(last);

        // final dispatch for execution
        if (!planListeners.isEmpty())
            root.listeners().addAll(planListeners);
        BIt<R> it = root.execute(rowType);
        m.dispatchNs = nanoTime()-entryNs-m.selectionAndAgglutinationNs-m.optimizationNs;

        // deliver metrics
        for (var l : fedListeners)
            l.accept(m);
        return it;
    }

    private static Plan copyUnwrap(Plan parent) {
        if (parent.left == null) return new Empty(parent);
        return parent.left.transform(sanitizeTransformer, null);
    }
    private static Plan mutateUnwrap(Plan parent) {
        return mutateSanitize(parent.left == null ? new Empty(parent) : parent.left);
    }

    private static final Plan.Transformer<Void> sanitizeTransformer = new Plan.Transformer<>() {
        @Override public Plan before(Plan parent, Void ctx) {
            Operator t = parent.type;
            return switch (t) {
                case JOIN, UNION -> {
                    if (parent.right == null) yield copyUnwrap(parent);
                    Plan copy = parent.copy();
                    int nFlat = -1, n = copy.opCount(), c = parent instanceof Union u ? u.crossDedupCapacity : -1;
                    for (int i = 0; i < n; i++) {
                        Plan o = copy.op(i).transform(this, ctx);
                        copy.replace(i, o);
                        if (o.type == t && (!(o instanceof Union u) || u.crossDedupCapacity == c))
                            nFlat = Math.max(0, nFlat) + o.opCount()-1;
                    }
                    if (nFlat == -1)
                        yield copy;

                    Plan[] flat = new Plan[n+nFlat];
                    nFlat = 0;
                    for (int i = 0; i < n; i++) {
                        Plan o = copy.op(i);
                        if (o.type == t && (!(o instanceof Union u) || u.crossDedupCapacity == c))
                            for (int j = 0, m = o.opCount(); j < m; j++) flat[nFlat++] = o.op(j);
                        else
                            flat[nFlat++] = o;
                    }
                    copy.replace(flat);
                    yield copy;
                }
                case QUERY -> {
                    Query q = (Query) parent;
                    if (!(q.client instanceof UnboundSparqlClient)) yield q;
                    var sq = (q).sparql;
                    if (sq instanceof Plan p) yield p.transform(this, ctx);
                    yield mutateSanitize(new SparqlParser().parse(q.sparql()));
                }
                default -> parent;
            };
        }

        @Override public Plan after(Plan parent, Void context, boolean copied) {
            if (parent instanceof Modifier m && m.isNoOp())
                return parent.left;
            return copied ? parent : parent.copy();
        }
    };

    static Plan copySanitize(Plan plan) {
        return plan.transform(sanitizeTransformer, null);
    }

    static Plan mutateSanitize(Plan plan) {
        var type = plan.type;
        return switch (type) {
            case JOIN -> {
                if (plan.right == null) yield mutateUnwrap(plan);
                int nFlat = -1, n = plan.opCount(), c = plan instanceof Union u ? u.crossDedupCapacity : -1;
                for (int i = 0; i < n; i++) {
                    Plan o = mutateSanitize(plan.op(i));
                    plan.replace(i, o);
                    if (o.type == type && (!(o instanceof Union u) || u.crossDedupCapacity == c))
                        nFlat = Math.max(0, nFlat) + o.opCount()-1;
                }
                if (nFlat == -1) yield plan;

                Plan[] flat = new Plan[n + nFlat];
                nFlat = 0;
                for (int i = 0; i < n; i++) {
                    Plan o = plan.op(i);
                    if (o.type == type && (!(o instanceof Union u) || u.crossDedupCapacity == c))
                        for (int j = 0, m = o.opCount(); j < m; j++) flat[nFlat++] = o.op(j);
                    else
                        flat[nFlat++] = o;
                }

                plan.replace(flat);
                yield plan;
            }
            case QUERY -> {
                var q = (Query)plan;
                if (!(q.client instanceof UnboundSparqlClient)) yield plan;
                SparqlQuery sq = q.sparql;
                Plan parsed = sq instanceof Plan p ? p.deepCopy()
                            : new SparqlParser().parse(sq.sparql());
                yield mutateSanitize(parsed);
            }
            case MODIFIER -> {//noinspection DataFlowIssue
                Plan left = mutateSanitize(plan.left);
                plan.left = left;
                yield ((Modifier)plan).isNoOp() ? left : plan;
            }
            default -> {
                Plan copy = plan.copy(null);
                copy.left  = plan.left  == null ? null : mutateSanitize(plan.left );
                copy.right = plan.right == null ? null : mutateSanitize(plan.right);
                yield copy;
            }
        };
    }

    private Plan plan(Plan plan, FedMetrics metrics) {
        int nOps = plan.opCount(), nSrc = sources.size();
        assert false : "fill metrics";
        return switch (plan) {
            case Join join -> {
                if      (((nOps-1|nSrc-1) & ~63) !=   0) yield coldestPlan(join, metrics);
                else if (nOps*nSrc               >   64) yield coldPlan(join, metrics);
                long sourced = 0, nonExcl = 0; // bit i <--> ops.get(i) has >0, >1 sources
                long op2src = 0; // bit o*nSrc+s <--> sources[s] matched ops[o]
                long src2op = 0; // bit s*nOps+o <--> sources[s] matched ops[o]
                for (int s = 0; s < nSrc; s++) {
                    long matched = sources.get(s).selector.subBitset(plan);
                    nonExcl |= sourced & matched;
                    sourced |= matched;
                    for (int o=0; (o=numberOfTrailingZeros(matched>>>o)) < 64; ++o) {
                        op2src |= 1L << (o*nSrc + s);
                        src2op |= 1L << (s*nOps + o);
                    }
                }

                // find safe unsourced operands. short-circuit if we have an unsourced TriplePattern
                long nonTP = ~sourced; // bit i <--> !(plan.op(i) instanceof TriplePattern)
                if (nonTP != 0 && isUnsourcedFailure(nonTP, plan)) yield new Empty(plan);

                // find sources with >0 exclusive operands
                long exclusive = sourced & ~nonExcl;
                long srcWExcl = 0;
                for (int o = 0; (o=numberOfTrailingZeros(exclusive>>>o)) < 64; o++)
                    srcWExcl |= op2src>>>o*nSrc;
                srcWExcl &= -1 >>> (64-nSrc);

                // allocate Plan[] bound with exact length
                Plan[] bound = new Plan[bitCount(nonTP)+bitCount(nonExcl)+bitCount(srcWExcl)];
                int nBound = 0;

                // add exclusive groups
                for (int s = 0; (s=numberOfTrailingZeros(srcWExcl>>>s)) < 64; s++) {
                    long sExclusive = src2op>>>(s*nOps) & exclusive;
                    bound[nBound++] = bindExclusive(sources.get(s).client, sExclusive, plan);
                }

                // add non-exclusive Unions
                for (int o=0; (o=numberOfTrailingZeros(nonExcl>>>o)) < 64; ++o)
                    bound[nBound++] = bindToSources(plan.op(o), op2src>>o*nSrc, cdc);

                if (nonTP != 0)
                    addNonTP(plan, nonTP, bound, nBound);
                plan.replace(bound);
                yield plan;
            }
            case TriplePattern t -> {
                if (nSrc > 64) yield coldPlan(t, metrics);
                long relevant = 0;
                for (int i = 0; i < nSrc; i++)
                    if (sources.get(i).selector.has(t)) relevant |= 1L << i;
                yield bindToSources(t, relevant, cdc);
            }
            default -> {
                if (nOps == 0) yield plan;
                for (int i = 0; i < nOps; i++) {
                    Plan o = plan.op(i), bound = plan(o, metrics);
                    if (bound != o) plan.replace(i, bound);
                }
                yield plan;
            }
        };
    }

    private static Plan bindExclusive(SparqlClient client, long subset, Plan parent) {
        int n = bitCount(subset), first = numberOfTrailingZeros(subset);
        if (n == 1) return parent.op(first);
        if (n == 2) {
            int second = numberOfTrailingZeros(subset >>> first + 1);
            return new Query(new Join(null, parent.op(first), parent.op(second)), client);
        }
        Plan[] ops = new Plan[n];
        for (int dst = 0, op = 0; (op=numberOfTrailingZeros(subset>>>op)) < 64; ++op)
            ops[dst] = parent.op(op);
        return new Query(new Join(null, ops), client);
    }

    private Plan bindToSources(Plan tp, long srcSubset, int crossDedupCapacity) {
        int n = bitCount(srcSubset), fIdx = numberOfTrailingZeros(srcSubset          );
        int                          sIdx = numberOfTrailingZeros(srcSubset>>>fIdx);
        Query fstQ = new Query(tp, sources.get(fIdx).client);
        Query sndQ = new Query(tp, sources.get(sIdx).client);
        if (n == 2)
            return new Union(crossDedupCapacity, fstQ, sndQ);
        Plan[] ops = new Plan[n];
        ops[0] = fstQ;
        ops[1] = sndQ;
        for (int o = 2, s = sIdx+1; (s=numberOfTrailingZeros(srcSubset>>>s)) < 64; s++)
            ops[o++] = new Query(tp, sources.get(s).client);
        return new Union(crossDedupCapacity, ops);
    }

    private Plan bindToSources(Plan tp, long[] op2src, int oIdx, int nOps, int nSrc,
                               int cdc) {
        int begin = oIdx*nSrc, nOpSources = cardinality(op2src, begin, begin+nOps);
        if (nOpSources == 2) {
            int i0 = nextSetOrLen(op2src, begin);
            return new Union(cdc, new Query(tp, sources.get(i0-begin).client),
                                  new Query(tp, sources.get(nextSetOrLen(op2src, i0+1)-begin).client));
        } else {
            var unionOps = new Plan[nOpSources];
            nOpSources = 0;
            for (int s = 0; (s=nextSetOrLen(op2src, begin+s)-begin) < nSrc; s++)
                unionOps[nOpSources++] = new Query(tp, sources.get(s).client);
            return new Union(cdc, unionOps);
        }
    }

    private static boolean isUnsourcedFailure(long[] sourced, Plan parent) {
        for (int o=0, n=parent.opCount(); (o=nextClear(sourced, o)) < n; ++o)
            if (parent.op(o) instanceof TriplePattern) return true;
        return false;
    }

    private static boolean isUnsourcedFailure(long unsourced, Plan parent) {
        for (int o = 0; (o=numberOfTrailingZeros(unsourced>>>o)) < 64; o++) {
            if (parent.op(o) instanceof TriplePattern) return true;
        }
        return false;
    }

    private static void addNonTP(Plan parent, long nonTP, Plan[] bound, int boundSize) {
        for (int o=0; (o=numberOfTrailingZeros(nonTP>>>o)) < 64; ++o)
            bound[boundSize++] = parent.op(o);
    }

    private static void addNonTP(Plan parent, long[] sourced, Plan[] bound, int boundSize) {
        for (int o = 0, n = parent.opCount(); (o=nextClear(sourced, o)) < n; o++)
            bound[boundSize++] = parent.op(o);
    }

    private Plan trivialPlan(Plan plan) {
        return switch (plan.type) {
            case JOIN -> {
                var cli = sources.get(0).client;
                var sel = sources.get(0).selector;
                for (int i = 0, n = plan.opCount(); i < n; i++) {
                    Plan o = plan.op(i);
                    if (o instanceof TriplePattern tp) {
                        if (!sel.has(tp)) yield new Empty(plan);
                        plan.replace(i, new Query(tp, cli));
                    } else {
                        plan.replace(i, trivialPlan(o));
                    }
                }
                yield plan;
            }
            case TRIPLE -> sources.get(0).selector.has((TriplePattern)plan)
                         ? new Query(plan, sources.get(0).client) : new Empty(plan);
            default -> {
                for (int i = 0, n = plan.opCount(); i < n; i++) {
                    Plan o = plan.op(i), bound = trivialPlan(o);
                    if (bound != o)
                        plan.replace(i, trivialPlan(plan.op(i)));
                }
                yield plan;
            }
        };
    }

    private Plan trivialPlan(Plan plan, FedMetrics metrics) {
        if (sources.size() == 0) return new Empty(plan);
        trivialPlan(plan);
        return plan;
    }

    private Plan coldPlan(Join join, FedMetrics metrics) {
        int nOps = join.opCount(), nSrc = sources.size(), cdc = dedupCapacity();
        long sourced = 0, nonExcl = 0, ssStartNs = nanoTime();
        long[] op2src       = new long[(nOps*nSrc-1 >> 6) + 1];
        long[] src2op       = new long[(nSrc*nOps-1 >> 6) + 1];
        for (int s = 0; s < nSrc; s++) {
            long matched = sources.get(s).selector.subBitset(join);
            nonExcl |= sourced & matched;
            sourced |= matched;
            for (int o = 0; (o=numberOfTrailingZeros(matched>>>o)) < 64; o++) {
                int i = o*nSrc+s;
                op2src[i>>6] |= 1L << i;
                i = s*nOps+o;
                src2op[i>>6] |= 1L << i;
            }
        }

        // find safe unsourced operands. short-circuit if we have an unsourced TriplePattern
        long nonTP = ~sourced;
        if (nonTP != 0 && isUnsourcedFailure (nonTP, join)) return new Empty(join);

        // find sources with >0 exclusive operands
        long exclusive = sourced & ~nonExcl, srcWExcl = 0;
        for (int o = 0, begin; (o=numberOfTrailingZeros(exclusive>>>o)) < 64; o++) {
            begin = o*nSrc;
            srcWExcl |= BS.get(op2src, begin, begin+nSrc);
        }

        // allocate bound array with exact size
        Plan[] bound = new Plan[bitCount(nonTP) + bitCount(nonExcl) + bitCount(srcWExcl)];
        int nBound = 0;

        // add exclusive groups
        for (int s = 0; (s=numberOfTrailingZeros(srcWExcl>>>s)) < 64; s++) {
            int begin = s * nOps;
            long sExclusive = BS.get(src2op, begin, begin+nOps);
            bound[nBound++] = bindExclusive(sources.get(s).client, sExclusive, join);
        }

        // add non-exclusive Unions
        for (int o = 0; (o = numberOfTrailingZeros(nonExcl>>>o)) < 64; o++) {
            int begin = o * nSrc;
            bound[nBound++] = bindToSources(join.op(o), get(op2src, begin, begin+nSrc), cdc);
        }

        if (nonTP != 0)
            addNonTP(join, nonTP, bound, nBound);
        join.replace(bound);
        return join;
    }

    private Plan coldestPlan(Join join, FedMetrics metrics) {
        int nOps = join.opCount(), nSrc = sources.size();
        long[] sourced   = new long[(nOps-1 >> 6) + 1];
        long[] nonExcl   = new long[(nOps-1 >> 6) + 1];
        long[] op2src    = new long[(nOps*nSrc-1 >> 6) + 1];
        long[] src2op    = new long[(nSrc*nOps-1 >> 6) + 1];
        long[] tmp       = new long[(Math.max(nOps,nSrc)-1 >> 6) + 1];
        for (int s = 0; s < nSrc; s++) {
            if (sources.get(s).selector.subBitset(tmp, join) == 0) continue;
            orAnd(nonExcl, tmp, sourced);
            or(sourced, tmp);
            for (int o = 0; (o=nextSet(tmp, o)) != -1; ) {
                BS.set(op2src, o*nSrc+s);
                BS.set(src2op, s*nOps+o);
            }
        }

        // count unsourced operands, short-circuit if any is a TriplePattern
        int nonTP = nOps - cardinality(sourced);
        if (nonTP > 0 && isUnsourcedFailure(sourced, join)) return new Empty(join);

        // find sources with exclusive operands
        Arrays.fill(tmp, 0L); //tmp is "sourcesWithExcl"
        for (int o = 0; (o=nextClear(nonExcl, o)) < nOps; o++) {
            int s = nextSet(op2src, o * nSrc);
            tmp[s>>6] |= 1L << s;
        }

        // allocate bound array
        Plan[] bound = new Plan[cardinality(nonExcl) + cardinality(tmp) + nonTP];
        int nBound = 0;

        // add exclusive groups
        for (int s = 0; (s=nextSet(tmp, s)) != -1; s++) {
            var sc = sources.get(s).client;
            int begin = s*nOps, nExclOps = cardinality(src2op, begin, begin+nOps);
            if (nExclOps < 3) {
                var f = new Query(join.op(begin = nextSet(src2op, begin)), sc);
                if (nExclOps == 1)
                    bound[nBound++] = f;
                else
                    bound[nBound++] = new Join(null, f, new Query(join.op(nextSet(src2op, ++begin)), sc));
            } else {
                Plan[] joinOps = new Plan[nExclOps];
                nExclOps = 0;
                for (int o = 0; (o = nextSet(src2op, o)) < nOps; o++)
                    joinOps[nExclOps++] = new Query(join.op(o), sc);
                bound[nBound++] = new Join(null, joinOps);
            }
        }

        //add non-exclusive groups
        int cdc = dedupCapacity();
        for (int o = 0; (o=nextSet(nonExcl, o)) != -1; o++)
            bound[nBound++] = bindToSources(join.op(o), op2src, o, nOps, nSrc, cdc);

        if (nonTP > 0)
            addNonTP(join, sourced, bound, nBound);
        join.replace(bound);
        return join;
    }

    private Plan coldPlan(TriplePattern tp, FedMetrics metrics) {
        int nSrc = sources.size();
        long[] relevant = new long[(nSrc - 1 >> 6) + 1];
        for (int s = 0; s < nSrc; s++)
            if (sources.get(s).selector.has(tp)) relevant[s>>6] |= 1L << s;
        return bindToSources(tp, relevant, 0, 1, nSrc, dedupCapacity());
    }
}
