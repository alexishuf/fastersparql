package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class BItProducer<B extends Batch<B>> extends ProducerTask<B> {
    private final BIt<B> it;
    private @Nullable ExhaustReason exhausted;

    public BItProducer(BIt<B> it, RecurringTaskRunner runner) {
        super(runner);
        this.it = it;
    }

    @Override protected B produce(long limit, long deadline, @Nullable B offer) throws Throwable {
        try {
            B b = it.nextBatch(offer);
            if (b == null && exhausted == null)
                exhausted = ExhaustReason.COMPLETED;
            return b;
        } catch (BItReadClosedException e) {
            if (exhausted == null)
                exhausted = ExhaustReason.CANCELLED;
            return null;
        }
    }

    @Override protected ExhaustReason exhausted() { return exhausted; }

    @Override protected void cleanup(@Nullable Throwable reason) {
        it.close();
    }
}
