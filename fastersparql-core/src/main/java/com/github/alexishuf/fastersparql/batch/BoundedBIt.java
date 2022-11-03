package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.BoundedBufferedBIt;
import org.checkerframework.common.returnsreceiver.qual.This;

/**
 * An asynchronous {@link BIt} that internally holds a bounded queue of batches.
 */
public interface BoundedBIt<T> extends BIt<T> {
    /**
     * Set the maximum number of ready batches at any point. The default is
     * {@link Integer#MAX_VALUE}, meaning there is no limit.
     */
    @This BoundedBufferedBIt<T> maxReadyBatches(int max);

    /** The current value set for {@link BoundedBIt#maxReadyBatches(int)}. */
    @SuppressWarnings("unused") int maxReadyBatches();

    /**
     * Set the maximum number of elements distributed across all buffered ready batches.
     * The default is {@link Long#MAX_VALUE}, meaning there is no limit.
     */
    @This BoundedBufferedBIt<T> maxReadyItems(long max);

    /** The current value set for {@link BoundedBIt#maxReadyItems(long)}. */
    @SuppressWarnings("unused") long maxReadyItems();
}
