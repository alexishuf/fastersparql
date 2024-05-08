package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface HasFillingBatch<B extends Batch<B>> {
    /**
     * If this {@link BatchQueue} or {@link Receiver} has a non-null (even if empty) filling
     * batch ready, return it. Else, return {@code null}.
     */
    @Nullable Orphan<B> pollFillingBatch();
}
