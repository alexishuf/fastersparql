package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadCancelledException;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.client.ItBindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class BindingBIt<B extends Batch<B>> extends AbstractFlatMapBIt<B> {
    private static final Supplier<ArrayList<SparqlClient.Guard>> GUARDS_FAC = new Supplier<>() {
        @Override public ArrayList<SparqlClient.Guard> get() {return new ArrayList<>();}
        @Override public String toString() {return "BindingBIt.GUARDS_FAC";}
    };
    @SuppressWarnings("unchecked")
    private static final Class<ArrayList<SparqlClient.Guard>> GUARDS_CLASS
            = (Class<ArrayList<SparqlClient.Guard>>) (Object)ArrayList.class;
    private static final Alloc<ArrayList<SparqlClient.Guard>> GUARDS_ALLOC
            = new Alloc<>(GUARDS_CLASS, "BindingBIt.GUARDS_ALLOC",
                          Alloc.THREADS*32, GUARDS_FAC, 16 + 2*4);
    static {Primer.INSTANCE.sched(GUARDS_ALLOC::prime);}

    protected final BatchMerger<B, ?> merger;
    protected final ItBindQuery<B> bindQuery;
    private @Nullable B lb, rb;
    private int leftRow = -1;
    private final BIt<B> empty;
    private final BatchBinding tempBinding;
    private long bindingSeq;
    private @Nullable ArrayList<SparqlClient.Guard> guards;

    /* --- --- --- lifecycle --- --- --- */

    public BindingBIt(ItBindQuery<B> bindQuery, @Nullable Vars projection) {
        super(projection != null ? projection : bindQuery.resultVars(),
              EmptyBIt.of(bindQuery.bindings.batchType()));
        this.guards = GUARDS_ALLOC.create();
        var left = bindQuery.bindings;
        Vars leftPublicVars = left.vars();
        Vars rFree = bindQuery.query.publicVars().minus(leftPublicVars);
        this.lb          = batchType.create(leftPublicVars.size()).takeOwnership(this);
        this.bindQuery   = bindQuery;
        this.empty       = inner;
        this.merger      = batchType.merger(vars(), leftPublicVars, rFree).takeOwnership(this);
        this.tempBinding = new BatchBinding(leftPublicVars);
        this.metrics     = bindQuery.metrics;
    }

    protected void addGuard(SparqlClient.Guard g) {
        //noinspection DataFlowIssue guards != null
        guards.add(g);
    }

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return Stream.of(bindQuery.bindings, inner);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (guards != null) {
            for (var g : guards) {
                try {
                    g.close();
                } catch (Throwable t) { reportCleanupError(t); }
            }
            guards = GUARDS_ALLOC.offer(guards);
        }
        lb = Batch.safeRecycle(lb, this);
        rb = Batch.safeRecycle(rb, this);
        Owned.safeRecycle(merger, this);
        if (cause != null) {
            if (inner != empty)
                inner.tryCancel();
            bindQuery.bindings.tryCancel();
        }
    }

    @Override public void close() {
        super.close();
        bindQuery.bindings.close();
        inner.close();
        lb = Batch.safeRecycle(lb, this);
        rb = Batch.safeRecycle(rb, this);
    }

    @Override public boolean tryCancel() {
        boolean did = super.tryCancel();
        bindQuery.bindings.tryCancel();
        inner.tryCancel();
        return did;
    }

    /* --- --- --- delegate control --- --- --- */

    /* --- --- --- binding behavior --- --- --- */
    protected abstract BIt<B> bind(BatchBinding binding);

    private static final byte PUB_MERGE           = 0x1;
    private static final byte PUB_LEFT            = 0x2;
    private static final byte PUB_MASK            = PUB_MERGE|PUB_LEFT;
    private static final byte CANCEL              = 0x4;
    private static final byte PUB_LEFT_AND_CANCEL = PUB_LEFT|CANCEL;

    @Override public Orphan<B> nextBatch(@Nullable Orphan<B> orphan) {
        B b = orphan == null ? null : orphan.takeOwnership(this).clear(nColumns);
        lock();
        try {
            if (plainState.isTerminated())
                return null; // cancelled
            long startNs = needsStartTime ? Timestamp.nanoTime() : Timestamp.ORIGIN;
            boolean rightEmpty = false, bindingEmpty = false;
            if (b == null)
                b = batchType.create(nColumns).takeOwnership(this);
            do {
                if (inner == empty) {
                    if (lb != null && ++leftRow >= lb.rows) {
                        leftRow = 0;
                        if ((lb=lb.dropHead(this)) == null) {
                            Orphan<B> nlb;
                            unlock();
                            try {
                                nlb = bindQuery.bindings.nextBatch(null);
                            } finally { lock(); }
                            if (nlb != null && plainState == State.ACTIVE) {
                                lb = nlb.takeOwnership(this);
                            } else {
                                Orphan.recycle(nlb);
                                break; // reached end or cancelled
                            }
                        }
                    }
                    inner = bind(tempBinding.attach(lb, leftRow));
                    rightEmpty = true;
                    bindingEmpty = true;
                }
                B rb = this.rb;
                this.rb = null;
                unlock();
                try {
                    rb = inner.nextBatch(rb, this);
                } catch (BItReadCancelledException e) {
                    if (isTerminated())
                        break;
                    throw e;
                } finally {
                    lock();
                }
                if (plainState.isTerminated()) {
                    Batch.recycle(rb, this);
                    break;
                } else {
                    this.rb = rb;
                }
                if      (rb      == null) inner = empty;
                else if (rb.rows >     0) rightEmpty = false;
                byte action = switch (bindQuery.type) {
                    case JOIN             -> rb != null               ? PUB_MERGE : 0;
                    case LEFT_JOIN        -> rb != null || rightEmpty ? PUB_MERGE : 0;
                    case EXISTS           -> rb != null ? PUB_LEFT_AND_CANCEL : CANCEL;
                    case NOT_EXISTS,MINUS -> rb == null ? PUB_LEFT_AND_CANCEL : CANCEL;
                };
                bindingEmpty &= (action&PUB_MASK) == 0;
                if ((action&PUB_MERGE) != 0)
                    b = merger.merge(b.releaseOwnership(this), lb, leftRow, rb).takeOwnership(this);
                else if ((action&PUB_LEFT)  != 0)
                    b.putRow(lb, leftRow);
                if ((action&CANCEL) != 0) {
                    inner.tryCancel();
                    inner = empty;
                }
                if (inner == empty) {
                    long seq = bindingSeq++;
                    if (bindingEmpty) bindQuery.   emptyBinding(seq);
                    else    bindQuery.nonEmptyBinding(seq);
                }
            } while (readyInNanos(b.totalRows(), startNs) > 0 && !plainState.isTerminated());
            return b.rows == 0 ? handleEmptyBatch(b) : onNextBatch(b.releaseOwnership(this));
        } catch (Throwable t) {
            onTermination(t);
            throw t;
        } finally {
            unlock();
        }
    }

    @SuppressWarnings("SameReturnValue") private Orphan<B> handleEmptyBatch(B batch) {
        batch.recycle(this);
        if (!plainState.isTerminated())
            onTermination(null);
        return null;
    }

    /* --- --- --- toString() --- --- --- */

    @Override protected String toStringNoArgs() {
        return super.toStringNoArgs()+'['+bindQuery.type+']';
    }

    @Override public String toString() {
        return toStringNoArgs()+'('+bindQuery.bindings+", "+bindQuery.query+')';
    }
}
