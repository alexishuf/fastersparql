package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.dedup.BTreeDedup;
import com.github.alexishuf.fastersparql.batch.dedup.SortProjection;
import com.github.alexishuf.fastersparql.emit.Emitters;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Modifier;
import com.github.alexishuf.fastersparql.operators.plan.OrderBy;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public abstract sealed class BatchSorter<B extends Batch<B>>
        extends BatchProcessor<B, BatchSorter<B>>
        implements ThrowingConsumer<B, Throwable> {
    private static final Logger log = LoggerFactory.getLogger(BatchSorter.class);

    private @Nullable BTreeDedup<B> tree;
    private final boolean dedup;
    private final OrderBy orderBy;
    private @Nullable SPSCBIt<B> queue;

    private static final int TERMINAL_CANCEL_MASK = IS_CANCEL_REQ|EXPECT_CANCELLED;
    private static final int DELIVERING_SORTED = 0x40000000;
    private static final Flags SORTER_FLAGS = PROC_FLAGS.toBuilder()
            .flag(DELIVERING_SORTED, "DELIVERING_SORTED")
            .build();

    protected BatchSorter(BatchType<B> bt, Vars allVars, int maxSize,
                          boolean dedup, OrderBy orderBy) {
        super(bt, allVars, CREATED, SORTER_FLAGS, null);
        int cols     = allVars.size();
        var sp       = orderBy.fill(allVars, SortProjection.ofByte(cols)).build();
        this.tree    = BTreeDedup.create(bt, cols, maxSize, !dedup, sp).takeOwnership(this);
        this.dedup   = dedup;
        this.orderBy = orderBy;
    }
    public static <B extends Batch<B>> Orphan<BatchSorter<B>>
    create(BatchType<B> bt, Modifier m) {
        boolean dedup = m.distinct != null;
        return new Concrete<>(bt, m.left().publicVars(), Integer.MAX_VALUE, dedup, m.orderBy);
    }
    public static <B extends Batch<B>> Orphan<BatchSorter<B>>
    create(BatchType<B> bt, Vars allVars, int maxSize, boolean dedup, OrderBy orderBy) {
        return new Concrete<>(bt, allVars, maxSize, dedup, orderBy);
    }
    private static final class Concrete<B extends Batch<B>>
            extends BatchSorter<B> implements Orphan<BatchSorter<B>> {
        public Concrete(BatchType<B> batchType, Vars allVars, int maxSize,
                        boolean dedup, OrderBy orderBy) {
            super(batchType, allVars, maxSize, dedup, orderBy);
        }
        @Override public BatchSorter<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }
    @Override protected void doRelease() {
        tree = Owned.safeRecycle(tree, this);
        super.doRelease();
    }

    private BTreeDedup<B> tree() { return requireNonNull(tree).requireOwner(this); }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        super.rebind(binding);
        tree().clear();
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = new StringBuilder();
        if (type == StreamNodeDOT.Label.MINIMAL)
            return StreamNodeDOT.minimalLabel(sb, this).toString();
        if (dedup)
            sb.append("Distinct(");
        sb.append("OrderBy(");
        for (int i = 0; i < orderBy.size(); i++) {
            if (orderBy.isDesc(i)) sb.append("DESC ");
            sb.append('?').append(orderBy.get(i)).append(' ');
        }
        if (!orderBy.isEmpty())
            sb.setLength(sb.length()-1);
        sb.append(')');
        if (dedup)
            sb.append(')');
        sb.append('@').append(Integer.toHexString(System.identityHashCode(this)));
        if (type.showState())
            sb.append("\nstate=").append(flags.render(state()));
        if (type.showStats())
            stats.appendToLabel(sb);
        if (before != null)
            sb.append('\n').append(before.label(type).replace("\n", "\n  "));
        return sb.toString();
    }

    @Override public void request(long rows) throws NoReceiverException {
        int st;
        var up = this.upstream;
        if (up != null && ((st=statePlain())&IS_INIT) != 0 && moveStateRelease(st, ACTIVE))
            up.request(Long.MAX_VALUE);
    }

    @Override public Orphan<B> processInPlace(Orphan<B> orphan) {
        B b = sort(orphan);
        b.clear();
        return b.releaseOwnership(this);
    }

    @Override public void onBatch(Orphan<B> orphan) {
        if (orphan != null)
            Batch.safeRecycle(sort(orphan), this);
    }

    private B sort(Orphan<B> orphan) {
        if (before != null)
            orphan = before.processInPlace(orphan);
        B batch = orphan.takeOwnership(this);
        if (beforeOnBatch(batch))
            tree().sort(batch);
        return batch;
    }

    @Override public void onBatchByCopy(B batch) {
        if (batch != null) {
            if (before != null) {
                onBatch(batch.dup());
            } else {
                if (beforeOnBatch(batch))
                    tree().sort(batch);
            }
        }
    }

    @Override public void onComplete() {
        if (tryDeliverSorted().notTerminated())
            super.onComplete();
    }

    @Override public void onCancelled() {
        if (tryDeliverSorted().notTerminated())
            super.onCancelled();
    }

    @Override public void onError(Throwable error) {
        if (tryDeliverSorted().notTerminated()) {
            super.onError(error);
        } else {
            log.error("Ignored error from upstream={} on {} state={}",
                      upstream, this, flags.render(state()), error);
        }
    }

    private enum DeliverSortedResult {
        TERMINATED, NOT_TERMINATED;
        public boolean notTerminated() {return this == NOT_TERMINATED;}
    }
    private DeliverSortedResult tryDeliverSorted() {
        if (!compareAndSetFlagRelease(DELIVERING_SORTED))
            return DeliverSortedResult.NOT_TERMINATED; // recursive call
        try {
            tree().destructiveForEach(this);
            return DeliverSortedResult.NOT_TERMINATED;
        } catch (BatchQueue.CancelledException e) {
            onCancelled();
            return DeliverSortedResult.TERMINATED;
        } catch (Throwable t) {
            onError(t);
            return DeliverSortedResult.TERMINATED;
        } finally {
            clearFlagsRelease(DELIVERING_SORTED);
        }
    }
    @Override public void accept(B b) throws Throwable {
        int st = statePlain();
        if ((st&DELIVERING_SORTED) == 0)
            throw new IllegalStateException("BatchSorter.accept() is not public");
        if ((st&TERMINAL_CANCEL_MASK) != 0)
            throw BatchQueue.CancelledException.INSTANCE; // cancel() or cancelUpstream()
        try {
            downstream.onBatchByCopy(b);
        } catch (Throwable t) {
            Emitters.handleEmitError(downstream, this, t, null);
            throw BatchQueue.CancelledException.INSTANCE; // stop iteration
        }
    }

    @Override protected void cancelUpstream() {
        super.cancelUpstream();
        SPSCBIt<B> q = queue;
        if (q != null)
            q.tryCancel();
    }

    @Override public boolean cancel() {
        boolean did = super.cancel();
        SPSCBIt<B> q = queue;
        if (q != null)
            q.tryCancel();
        return did;
    }

    @Override public @Nullable BIt<B> terminalResults() {
        var queue  = new SPSCBIt<>(batchType, vars);
        var tree   = requireNonNull(this.tree).transferOwnership(this, queue);
        this.tree  = null;
        this.queue = queue;
        Thread.startVirtualThread(() -> {
            Throwable err = null;
            try {
                tree.destructiveForEach(queue::copy);
            } catch (Throwable t) {
                err = t;
            } finally {
                if (!(err instanceof BatchQueue.QueueStateException && queue.isTerminated()))
                    queue.complete(err);
                Owned.safeRecycle(tree, queue);
            }
        });
        return queue;
    }
}
