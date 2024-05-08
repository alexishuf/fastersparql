package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

public abstract sealed class CollectingReceiver<B extends Batch<B>>
        extends ReceiverFuture<B, B, CollectingReceiver<B>> {
    private @Nullable B collected;

    public static <B extends Batch<B>> Orphan<CollectingReceiver<B>>
    create(Orphan<? extends Emitter<B, ?>> orphanUpstream) {
        return new Concrete<>(orphanUpstream);
    }
    protected CollectingReceiver(Orphan<? extends Emitter<B, ?>> orphanUpstream) {
        subscribeTo(orphanUpstream);
        var up = requireNonNull(this.upstream);
        collected = up.batchType().create(up.vars().size()).takeOwnership(this);
    }

    @Override public @Nullable CollectingReceiver<B> recycle(Object currentOwner) {
        super.recycle(currentOwner);
        collected = Batch.safeRecycle(collected, this);
        return null;
    }

    private static final class Concrete<B extends Batch<B>> extends CollectingReceiver<B>
            implements Orphan<CollectingReceiver<B>> {
        public Concrete(Orphan<? extends Emitter<B, ?>> orphanUpstream) {super(orphanUpstream);}
        @Override public CollectingReceiver<B> takeOwnership(Object newOwner) {
            return takeOwnership0(newOwner);
        }
    }

    public Orphan<B> take() {
        requireAlive();
        Orphan<B> orphan = requireNonNull(collected).releaseOwnership(this);
        collected = null;
        return orphan;
    }

    @Override public void onBatch(Orphan<B> batch) {
        requireAlive();
        requireNonNull(collected).append(batch);
    }
    @Override public void onBatchByCopy(B batch) {
        requireAlive();
        requireNonNull(collected).copy(batch);
    }
    @Override public void onComplete() {
        requireAlive();
        complete(requireNonNull(collected));
    }
}
