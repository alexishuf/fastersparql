package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.IdBatchBucket;

public class HdtBatchBucket extends IdBatchBucket<HdtBatch> {

    public HdtBatchBucket(int rowsCapacity, int cols) {
        super(HdtBatchType.INSTANCE.create(rowsCapacity, cols, 0), rowsCapacity);
    }

    @Override public BatchType<HdtBatch> batchType() { return HdtBatch.TYPE; }
}
