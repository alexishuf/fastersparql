package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.IdBatchBucket;

public class StoreBatchBucket extends IdBatchBucket<StoreBatch> {
    public StoreBatchBucket(int rowsCapacity, int cols) {
        super(StoreBatch.TYPE.create(rowsCapacity, cols), rowsCapacity);
    }

    @Override public BatchType<StoreBatch> batchType()             { return StoreBatch.TYPE; }
}
