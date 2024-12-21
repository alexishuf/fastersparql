package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.BindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.RopeFactory;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRopeView;
import com.github.alexishuf.fastersparql.operators.bit.PlanBindingBIt;
import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip;
import com.github.alexishuf.fastersparql.sparql.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.asFinal;
import static com.github.alexishuf.fastersparql.sparql.parser.SparqlParser.parse;
import static java.lang.Long.numberOfTrailingZeros;

public abstract sealed class GroupBind<B extends Batch<B>>
        extends AbstractOwned<GroupBind<B>> {
    public static final int GROUP_SIZE = FSProperties.sparqlGroupBindJoin();
    public static final boolean ENABLED = GROUP_SIZE > 1;

    private static final FinalSegmentRope IDX      = asFinal("__fastersparqlIdx");
    private static final FinalTerm[]      SEQ_TERM = new FinalTerm[GROUP_SIZE];
    static {
        if (GROUP_SIZE > 64)
            throw new ExceptionInInitializerError("GROUP_SIZE="+GROUP_SIZE+", expected <= 64");
        int  maxLen = 3+(int)Math.floor(Math.log10(GROUP_SIZE));
        for (int i = 0; i < GROUP_SIZE; i++) {
            var local = RopeFactory.make(maxLen).add('"').add(i).add('"').take();
            SEQ_TERM[i] = new FinalTerm(EMPTY, local, true);
        }
    }

    public final BindQuery<B> bindQuery;
    public final Vars resultVars;
    private B lb;
    private short groupBegin, groupSize;
    private int enqueuedLeftRows;
    private boolean groupStarted;
    private final short valuesPlanSeqCol;
    private final BindType bindType;
    private final Plan valuesPlan;
    private final Plan templatePlan;
    private final boolean rightPassThroughIfTemplatePlan;
    private final Vars templatePlanFreeVars;
    private @Nullable BatchMerger<B, ?> mergerIfTemplatePlan;
    private final BatchBinding templateBinding;
    public final BatchType<B> batchType;
    private long bindingSeq;
    private long nonEmptyGroupRows;
    private final short mergeSourcesLen;
    private short[] mergeSources;
    private final SegmentRopeView local;
    private final Values values;
    private TermBatch valuesBatch;
    private short[] valuesProjection;
    private final short valuesProjectionLen;
    private final boolean dedup;


    private GroupBind(BindQuery<B> bindQuery, Plan right, boolean weakDedup,
                      @Nullable SparqlClient clientIfNotDefined, @Nullable Vars projection) {
        this.batchType  = bindQuery.batchType();
        this.bindQuery  = bindQuery;
        this.bindType   = bindQuery.type;
        this.local      = new SegmentRopeView();
        this.resultVars = projection == null ? bindQuery.resultVars() : projection;
        this.dedup      = weakDedup;

        Vars bindingVars = bindQuery.bindingsVars();
        Vars.Mutable valuesVars = Vars.fromSet(bindingVars);
        valuesVars.retainAll(right.allVars());
        valuesVars.add(IDX);
        templateBinding     = new BatchBinding(bindQuery.bindingsVars());
        valuesProjectionLen = (short)(valuesVars.size()-1);
        valuesProjection    = ArrayAlloc.shortsAtLeast(valuesProjectionLen);
        for (int i = 0; i < valuesProjectionLen; i++)
            valuesProjection[i] = (short)bindingVars.indexOf(valuesVars.get(i));

        valuesBatch = TERM.create(valuesProjectionLen+1).takeOwnership(this);
        valuesBatch.beginPut();
        valuesBatch.putTerm(valuesProjectionLen, SEQ_TERM[0]);
        valuesBatch.commitPut();
        values = new Values(valuesVars, valuesBatch.releaseOwnership(this), true);

        templatePlanFreeVars = new Vars.Mutable(right.publicVars().size()+1);
        for (SegmentRope v : right.publicVars()) {
            if (!bindingVars.contains(v) && resultVars.contains(v))
                templatePlanFreeVars.add(v);
        }
        Vars boundPlanVars    = templatePlanFreeVars.union(IDX);
        this.valuesPlanSeqCol = (short)(boundPlanVars.size()-1);

        boolean dedup = right.isAsk() || weakDedup || !bindType.isJoin();
        valuesPlan   = wrapValues(right, values, boundPlanVars, dedup, clientIfNotDefined);
        templatePlan = wrapTemplate(right, templatePlanFreeVars, dedup, clientIfNotDefined);

        mergeSourcesLen = (short)resultVars.size();
        mergeSources = ArrayAlloc.shortsAtLeast(mergeSourcesLen);
        boolean rightPassThrough = mergeSourcesLen == templatePlanFreeVars.size();
        for (int i = 0, src; i < mergeSourcesLen; i++) {
            SegmentRope v = resultVars.get(i);
            if  ((src=bindingVars.indexOf(v)+1) == 0)
                src = -boundPlanVars.indexOf(v)-1;
            if (rightPassThrough && src >= 0 || -src-1 != i)
                rightPassThrough = false;
            mergeSources[i] = (short)src;
        }
        rightPassThroughIfTemplatePlan = rightPassThrough;
    }

    private static final class Concrete<B extends Batch<B>> extends GroupBind<B>
            implements Orphan<GroupBind<B>> {
        public Concrete(BindQuery<B> bindQuery, Plan right, boolean weakDedup,
                        @Nullable SparqlClient clientIfNotDefined,
                        @Nullable Vars projection) {
            super(bindQuery, right, weakDedup, clientIfNotDefined, projection);
        }
        @Override public GroupBind<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable GroupBind<B> recycle(Object currentOwner) {
        internalMarkGarbage(currentOwner);
        Orphan.safeRecycle(values.takeValues());
        valuesBatch          = null;
        valuesProjection     = ArrayAlloc.recycleShorts(valuesProjection);
        mergeSources         = ArrayAlloc.recycleShorts(mergeSources);
        mergerIfTemplatePlan = Owned.safeRecycle(mergerIfTemplatePlan, this);
        return null;
    }

    private static Plan wrapTemplate(SparqlQuery query, Vars templatePlanVars, boolean dedup,
                                     SparqlClient client) {
        boolean isBound = isBound(query);
        if (dedup)
            query = FS.distinctAtLeast(query, DistinctType.WEAK);
        query = FS.project(query, templatePlanVars);
        return isBound ? (Plan)query : new Query(query, client);
    }

    private static @PolyNull Plan
    wrapValues(@PolyNull Plan original, Values values, Vars boundPlanVars,
               boolean dedup, @Nullable SparqlClient client) {
        if (original == null) {
            return null;
        } else if (original instanceof Query q) {
            return wrapValuesForUnbound(parse(q.sparql), values, boundPlanVars, dedup, q.client);
        } else if (isBound(original)) {
            Plan[] replacement = new Plan[original.opCount()];
            for (int i = 0, n = original.opCount(); i < n; i++) {
                Plan op = original.op(i);
                replacement[i] = wrapValues(op, values, null, dedup, client);
            }
            var w = original.copy(replacement);
            if (w instanceof Union u && dedup)
                u.crossDedup = false;
            boolean mustProject = !w.publicVars().equals(boundPlanVars);
            boolean mustDedup = dedup && w.distinct() == null;
            if (mustProject || mustDedup) {
                if (w instanceof Modifier m) {
                    if (mustProject) m.projection = boundPlanVars;
                    if (mustDedup)   m.distinct   = DistinctType.WEAK;
                } else {
                    w = new Modifier(w, null, mustProject ? boundPlanVars : null,
                                     mustDedup ? DistinctType.WEAK : null,
                                     0, Long.MAX_VALUE, null);
                }
            }
            return w;
        } else if (client != null) {
            return wrapValuesForUnbound(original, values, boundPlanVars, dedup, client);
        } else {
            throw new IllegalArgumentException("No SparqlClient set for unbound query");
        }
    }
    private static Plan wrapValuesForUnbound(Plan original, Values values,
                                             @Nullable Vars boundPlanVars,
                                             boolean dedup, SparqlClient client) {
        var origMod = original instanceof Modifier m ? m : null;
        var body = origMod == null ? original : origMod.left();
        var join = new Join(values, body);
        Collection<?> filters;
        OrderBy ob;
        DistinctType distinct;
        if (origMod != null) {
            ob = origMod.orderBy;
            filters = origMod.filters;
            distinct = (dedup || origMod.isAsk()) && origMod.distinct == null
                    ? DistinctType.WEAK : origMod.distinct;
        } else {
            ob = null;
            filters = List.of();
            distinct = dedup ? DistinctType.WEAK : null;
        }
        Vars projection;
        if (boundPlanVars == null) {
            projection = Vars.fromSet(original.publicVars());
            projection.removeAll(values.publicVars());
            projection.add(IDX);
        } else {
            projection = boundPlanVars;
        }
        Plan query = FS.modifiers(join, ob, projection, distinct,
                                  0, Long.MAX_VALUE, filters);
        return new Query(query, client);
    }

    public static <B extends Batch<B>> @Nullable Orphan<GroupBind<B>>
    tryCreate(BindQuery<B> bindQuery, boolean weakDedup, Vars projection) {
        return tryCreate(bindQuery, weakDedup, null, projection);
    }

    public static <B extends Batch<B>> @Nullable Orphan<GroupBind<B>>
    tryCreate(BindQuery<B> bindQuery, boolean weakDedup,
              @Nullable SparqlClient clientIfNotDefined,
              @Nullable Vars projection) {
        if (ENABLED && canGroupBind(bindQuery.query, clientIfNotDefined)) {
            return new Concrete<>(bindQuery, parse(bindQuery.query),
                                  weakDedup, clientIfNotDefined, projection);
        }
        return null;
    }

    private static boolean isBound(Plan p) {
        return switch (p.type) {
            case QUERY,VALUES,EMPTY                -> true;
            case TRIPLE                            -> false;
            case MODIFIER                          -> isBound(p.left());
            case LEFT_JOIN,NOT_EXISTS,MINUS,EXISTS -> isBound(p.left()) && isBound(p.right());
            default -> {
                for (int i = 0, n = p.opCount(); i < n; i++) {
                    if (!isBound(p.op(i)))
                        yield false;
                }
                yield true;
            }
        };
    }
    private static boolean isBound(SparqlQuery query) {
        return query instanceof Plan p && isBound(p);
    }

    private static boolean canGroupBind(SparqlQuery query, @Nullable SparqlClient client) {
        if (client != null && (client.isLocalInProcess() || client.usesBindingAwareProtocol()))
            return false; // client is not suitable for group binding
        return canGroupBind0(query, client);
    }
    private static boolean canGroupBind0(SparqlQuery query, @Nullable SparqlClient client) {
        Plan plan;
        if (query instanceof Plan p) {
            if (query instanceof Query q)
                return canGroupBind(q.sparql, q.client);
            if (p.type == Operator.VALUES)
                return false;
            if (p.type == Operator.JOIN && p.right != null)
                return false;
            plan = p;
        } else {
            if (client == null)
                return false;
            if (query instanceof OpaqueSparqlQuery osq) {
                int len = osq.sparql.len;
                if (osq.sparql.skipUntil(0, len, SparqlSkip.OFFSET_u8) >= len
                        && osq.sparql.skipUntil(0, len, SparqlSkip.LIMIT_u8) >= len) {
                    return true; // must check OFFSET and LIMIT values
                }
            }
            plan = parse(query);
        }
        if (plan instanceof Modifier m && (m.offset != 0 || hasLimitOtherThanAsk(m)))
            return false; // inserting VALUES will violate OFFSET/LIMIT semantics
        boolean hasBound = false;
        for (int i = 0, n = plan.opCount(); i < n; i++) {
            Plan op = plan.op(i);
            if (!canGroupBind0(op, client))
                return false;
            if (isBound(op))
                hasBound = true;
            else if (hasBound)
                return false;
        }
        return true; // no violations
    }


    private static boolean hasLimitOtherThanAsk(Modifier m) {
        return m.limit != 1 && m.limit != Long.MAX_VALUE;
    }

    public static <B extends Batch<B>> Vars resultVars(Orphan<GroupBind<B>> orphan) {
        return ((GroupBind<?>)orphan).resultVars;
    }

    public static <B extends Batch<B>> BatchType<B> batchType(Orphan<GroupBind<B>> orphan) {
        //noinspection unchecked
        return ((GroupBind<B>)orphan).bindQuery.batchType();
    }

    public Vars bindableVars() {return templatePlan.allVars();}

    public void addGuards(ArrayList<SparqlClient.Guard> out) {
        PlanBindingBIt.scanClients(valuesPlan, out);
    }

    public Orphan<GroupBind<B>> bound(BatchBinding binding) {
        Plan bound      = templatePlan.bound(binding);
        Vars projection = resultVars.minus(binding.vars());
        return new Concrete<>(bindQuery, bound, dedup, null, projection);
    }

    public int enqueuedLeftRows() {return enqueuedLeftRows;}

    public @Nullable Orphan<B> enqueueLeftBatch(Orphan<B> b) {
        int rows = Batch.peekTotalRows(b);
        if (lb == null)
            lb = b.takeOwnership(this);
        else
            lb.append(b);
        enqueuedLeftRows += rows;
        return null;
    }

    public @Nullable Plan startNextGroup() {
        if (groupStarted)
            throw new IllegalStateException("group already started");
        if (lb == null)
            return null;
        groupSize = (short)Math.min(GROUP_SIZE, lb.rows-groupBegin);
        groupStarted = true;
        enqueuedLeftRows = Math.max(0, enqueuedLeftRows-groupSize);
        if (groupSize == 1) {
            return templatePlan.bound(templateBinding.attach(lb, groupBegin));
        } else {
            TermBatch vb = valuesBatch;
            vb.clear();
            for (short i = 0, size = groupSize; i < size; i++) {
                vb.beginPut();
                for (short dst = 0; dst < valuesProjectionLen; dst++)
                    vb.putTerm(dst, lb.get(groupBegin + i, valuesProjection[dst]));
                vb.putTerm(valuesProjectionLen, SEQ_TERM[i]);
                vb.commitPut();
            }
            return valuesPlan;
        }
    }

    public @Nullable Orphan<B> endCurrentGroup() {
        if (!groupStarted)
            throw new IllegalStateException("group not started");
        groupStarted = false;
        short gb = this.groupBegin, gs = this.groupSize;
        for (int i = 0; i < gs; i++) {
            if ((nonEmptyGroupRows&(-1L<<i)) == 0)
                bindQuery.emptyBinding(bindingSeq);
            else
                bindQuery.nonEmptyBinding(bindingSeq);
            bindingSeq++;
        }
        B b = switch (bindType) {
            case JOIN,EXISTS -> null;
            case LEFT_JOIN,MINUS,NOT_EXISTS -> {
                @Nullable B result = null;
                long missing = ~nonEmptyGroupRows;
                if ((missing&((1L<<gs)-1)) != 0) {
                    B out = batchType.create(mergeSourcesLen).takeOwnership(this);
                    for (int seq = 0; (seq+=numberOfTrailingZeros(missing>>>seq)) < gs; seq++) {
                        out.beginPut();
                        for (int dst = 0, src; dst < mergeSourcesLen; dst++) {
                            if ((src = mergeSources[dst]) > 0)
                                out.putTerm(dst, lb, gb + seq, src - 1);
                        }
                        out.commitPut();
                    }
                    result = out;
                }
                yield result;
            }
        };
        if ((gb+=gs) >= lb.rows) {
            lb = lb.dropHead(this);
            gb = 0;
        }
        groupBegin = gb;
        groupSize = 0;
        nonEmptyGroupRows = 0L;
        return b == null ? null : b.releaseOwnership(this);
    }

    public Orphan<B> processRightBatch(@Nullable Orphan<B> orphan) {
        if (orphan == null)
            return null;
        B in = orphan.takeOwnership(this);
        short gs = this.groupSize, gb = groupBegin;
        if (gs == 1)
            return processRightBatchFromTemplate(in, gb);
        boolean negation = bindType.isNegation();
        B out = batchType.create(mergeSourcesLen).takeOwnership(this);
        for (; in != null; in = in.dropHead(this)) {
            for (short r = 0, rows = in.rows; r < rows; r++) {
                int seq = -1;
                try {
                    if (in.localView(r, valuesPlanSeqCol, local))
                        seq = (int)local.parseLong(1);
                } catch (NumberFormatException ignored) {}
                if (seq < 0 || seq > gs)
                    throw new InvalidSparqlResultsException("Malformed sequence number");
                nonEmptyGroupRows |= 1L<<seq;
                if (negation)
                    continue;
                out.beginPut();
                short lr = (short) (gb+seq);
                for (short d = 0, s; d < mergeSourcesLen; d++) {
                    if      ((s=mergeSources[d]) > 0) out.putTerm(d, lb, lr, s-1);
                    else if ( s                  < 0) out.putTerm(d, in, r, -s-1);
                }
                out.commitPut();
            }
        }
        return out.releaseOwnership(this);
    }

    private Orphan<B> processRightBatchFromTemplate(B in, short lr) {
        if (in.rows != 0)
            nonEmptyGroupRows |= 1L;
        if (rightPassThroughIfTemplatePlan)
            return in.releaseOwnership(this);
        Orphan<B> out = batchType.create(mergeSourcesLen);
        if (!bindType.isNegation()) {
            BatchMerger<B, ?> merger = mergerIfTemplatePlan;
            if (merger == null)
                merger = createMergerIfTemplatePlan();
            out = merger.merge(out, lb, lr, in);
        }
        Batch.safeRecycle(in, this);
        return out;
    }

    private BatchMerger<B, ?> createMergerIfTemplatePlan() {
        var bv = bindQuery.bindingsVars();
        var m  = batchType.merger(resultVars, bv, templatePlanFreeVars).takeOwnership(this);
        mergerIfTemplatePlan = m;
        return m;
    }
}
