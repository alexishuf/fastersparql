package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.SingletonBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.stream.Stream;

public final class BItProducer<B extends Batch<B>> extends ProducerTask<B> {
    private final BIt<B> it;
    private @Nullable ExhaustReason exhausted;

    public BItProducer(BIt<B> it, AsyncEmitter<B> ae) {
        super(ae.runner,
              it instanceof EmptyBIt<B> || it instanceof SingletonBIt<B>
                         ? ae.preferredWorker : RR_WORKER);
        this.it = it;
        registerOn(ae);
    }

    @Override public Stream<? extends StreamNode> upstream() {
        return Stream.of(it);
    }

    @Override protected B produce(long limit, long deadline, @Nullable B offer) {
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

    @Override public void rebind(BatchBinding<B> binding) {
        throw new UnsupportedOperationException("BIt does not allow rebind()");
    }

    @Override protected void doRelease() {
        super.doRelease();
        it.close();
    }
}
