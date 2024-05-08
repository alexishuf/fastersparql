package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.util.owned.Orphan;

public interface BatchConverter<B extends Batch<B>> {

    /**
     * Get {@code in} itself or a transformed {@link BIt} that will yield the same batches
     * but represented as instance of {@code B} instead of instance of {@code I}.
     *
     * @param in the input {@link BIt}
     * @return a {@link BIt} over converted batches or {@code in} itself if {@code I} is or
     *         extends {@code B}
     * @param <I> The input batch type
     */
    <I extends Batch<I>> BIt<B>     convert(BIt<I>     in);

    /**
     * Get {@code in} itself or a transformed {@link Emitter} that will yield the same batches
     * but represented as instance of {@code B} instead of instance of {@code I}.
     *
     * @param in the input {@link Emitter}
     * @return a {@link Emitter} over converted batches or {@code in} itself if {@code I} is or
     *         extends {@code B}
     */
    <I extends Batch<I>> Orphan<? extends Emitter<B, ?>>
    convert(Orphan<? extends Emitter<I, ?>> in);
}
