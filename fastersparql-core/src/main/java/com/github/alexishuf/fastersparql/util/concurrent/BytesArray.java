package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.SpecialOwner;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.IntFunction;

import static com.github.alexishuf.fastersparql.batch.type.BatchType.PREFERRED_BATCH_TERMS;
import static com.github.alexishuf.fastersparql.util.concurrent.LevelAlloc.len2level;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;

/**
 * A wrapper for {@code Bytes[]}. Such a wrappers is necessary if there is indent to pool it.
 * Pooling  {@code Byte[]} instances will either lead to undetected leaks or to false
 * leak reports.
 */
public class BytesArray extends AbstractOwned<BytesArray> {
    public static final BytesArray EMPTY = create(0).takeOwnership(SpecialOwner.CONSTANT);
    private static final Fac FAC = new Fac();
    private static final Recycler RECYCLER = new Recycler();
    private static final LevelAlloc<BytesArray> ALLOC;
    private static final Logger log = LoggerFactory.getLogger(BytesArray.class);

    static {
        int batchLevel = len2level(PREFERRED_BATCH_TERMS);
        int maxLevel = len2level(Short.MAX_VALUE);
        ALLOC = new LevelAlloc<>(
                BytesArray.class, "BytesArray.ALLOC",
                20, 4, BytesArray.RECYCLER.clearElseMake,
                new LevelAlloc.Capacities()
                        .set(0, maxLevel, Alloc.THREADS*8)
                        .set(0, 3, Alloc.THREADS*32 )
                        .set(4, 4, Alloc.THREADS*128)
                        .set(5, 6, Alloc.THREADS*64 )
                        .set(batchLevel-3, batchLevel-3, Alloc.THREADS*64)
                        .set(batchLevel-2, batchLevel-1, Alloc.THREADS*128)
                        .set(batchLevel, batchLevel, Alloc.THREADS*64)
                        .set(0, len2level(PREFERRED_BATCH_TERMS), Alloc.THREADS*32)
        );
        ALLOC.setZero(BytesArray.FAC.apply(0));
        RECYCLER.pool = ALLOC;
    }

    public static Orphan<BytesArray> createFromLevel(int level) {
        return ALLOC.createFromLevel(level).releaseOwnership(RECYCLED);
    }
    public static Orphan<BytesArray> createAtLeast(int len) {
        return ALLOC.createAtLeast(len).releaseOwnership(RECYCLED);
    }
    public static BytesArray recycleAndGetEmpty(@Nullable BytesArray ba, Object owner) {
        if (ba != null) {
            try {
                ba.recycle(owner);
            } catch (Throwable t) {
                log.error("Failed to recycle:", t);
            }
        }
        return EMPTY;
    }

    public final Bytes[] arr;

    public static Orphan<BytesArray> create(int len) { return new Concrete(len); }
    private BytesArray(int n) {this.arr = new Bytes[n];}

    private static final class Concrete extends BytesArray implements Orphan<BytesArray> {
        private Concrete(int n) {super(n);}
        @Override public BytesArray takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable BytesArray recycle(Object currentOwner) {
        RECYCLER.sched(this.transferOwnership(currentOwner, RECYCLER), arr.length);
        return null;
    }

    public static final class Fac implements IntFunction<BytesArray> {
        @Override public BytesArray apply(int n) {
            return n == 0 ? EMPTY : BytesArray.create(n).takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "BytesArray.FAC";}
    }

    public static final class Recycler extends LevelCleanerBackgroundTask<BytesArray>
            implements SpecialOwner.Recycled {
        public Recycler() {super("BytesArray.RECYCLER", FAC);}
        @Override public String journalName() {return getName();}

        @Override protected void clear(BytesArray holder) {
            Bytes[] arr = holder.arr;
            for (int i = 0; i < arr.length; i++) {
                Bytes bytes = arr[i];
                if (bytes != null)
                    arr[i] = bytes.recycle(this);
            }
            holder.transferOwnership(this, RECYCLED);
        }
    }

}
