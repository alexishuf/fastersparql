package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.util.concurrent.CleanerBackgroundTask;
import com.github.alexishuf.fastersparql.util.owned.SpecialOwner;

import java.util.function.Supplier;

public class ObjBatchCleaner<B extends ObjBatch<B, ?>>
        extends CleanerBackgroundTask<B>
        implements SpecialOwner.Recycled {
    public ObjBatchCleaner(String name, Supplier<B> factory) {super(name, factory);}

    @Override public String journalName() {return getName();}

    public void sched(B node, Object currentOwner) {
        sched(node.transferOwnership(currentOwner, this));
    }

    @Override protected void clear(B b) {b.clearAndMarkRecycled(this);}

    @Override protected void handle(B o) {o.doRecycleToShared(this);}
}
