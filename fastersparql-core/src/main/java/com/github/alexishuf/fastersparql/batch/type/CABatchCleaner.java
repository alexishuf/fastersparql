package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.util.concurrent.CleanerBackgroundTask;
import com.github.alexishuf.fastersparql.util.owned.SpecialOwner;

import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.batch.type.BatchType.PREFERRED_BATCH_TERMS;

class CABatchCleaner extends CleanerBackgroundTask<CABatch> implements SpecialOwner.Recycled {
    public static final CABatchCleaner INSTANCE = new CABatchCleaner();

    static final class NewCABatchFactory implements Supplier<CABatch> {
        @Override public CABatch get() {
            return CABatch.createNotPooled(PREFERRED_BATCH_TERMS, 1).takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "NewCABatchFactory";}
    }
    public CABatchCleaner() {super("CABatchCleaner", new NewCABatchFactory());}

    public void sched(CABatch node, Object nodeOwner) {
        sched(node.transferOwnership(nodeOwner, this));
    }

    @Override public String journalName() {return getName();}

    @Override protected void  clear(CABatch b) {b.clearAndMarkRecycled(this);}
    @Override protected void handle(CABatch o) {o.completeAsyncRecycle(this);}
}
