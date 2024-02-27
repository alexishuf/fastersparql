package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DiscardingReceiver<B extends Batch<B>> extends ReceiverErrorFuture<B>{
    @Override public @Nullable B onBatch(B batch) {return batch;}
}
