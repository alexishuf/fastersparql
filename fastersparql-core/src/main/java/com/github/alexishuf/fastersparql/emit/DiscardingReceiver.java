package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.util.owned.Orphan;

public abstract sealed class DiscardingReceiver<B extends Batch<B>>
        extends ReceiverErrorFuture<B, DiscardingReceiver<B>>{
    public static <B extends Batch<B>> Orphan<DiscardingReceiver<B>>
    create(Orphan<? extends Emitter<B, ?>> upstream) {return new Concrete<>(upstream);}

    protected DiscardingReceiver(Orphan<? extends Emitter<B, ?>> upstream) {
        subscribeTo(upstream);
    }

    private static final class Concrete<B extends Batch<B>>
            extends DiscardingReceiver<B> implements Orphan<DiscardingReceiver<B>> {
        public Concrete(Orphan<? extends Emitter<B, ?>> upstream) {super(upstream);}
        @Override public DiscardingReceiver<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public void onBatch(Orphan<B> batch) {
        if (batch != null) batch.takeOwnership(this).recycle(this);
    }
    @Override public void onBatchByCopy(B batch) {}
}
