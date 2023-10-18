package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;

public final class CollectingReceiver<B extends Batch<B>> extends ReceiverFuture<B, B> {
    private B collected;

    public CollectingReceiver(Emitter<B> upstream) {
        collected = upstream.batchType().create(64, upstream.vars().size());
        subscribeTo(upstream);
    }

    public B collected() { return collected; }

    @Override public B onBatch(B batch) {
        collected = collected.put(batch);
        return batch;
    }

    @Override public void onRow(B batch, int row) { collected = collected.putRow(batch, row); }
    @Override public void onComplete()            { complete(collected); }
}
