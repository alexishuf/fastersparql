package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.type.ScopedIds.Scope;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import com.github.alexishuf.fastersparql.util.owned.SidecarOwned;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;

public abstract class ScopedIdBatchType extends IdBatchType<ScopedIdBatch> {
    private static final class ScopedIdBatchFac implements Supplier<ScopedIdBatch> {
        @Override public ScopedIdBatch get() {
            long[] ids = new long[PREFERRED_BATCH_TERMS];
            return new ScopedIdBatch.Concrete(ids, (short)1).takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "ScopedIdBatchType.FAC";}
    }
    private ScopedIdBatchType() {
        super(ScopedIdBatch.class, new ScopedIdBatchType.ScopedIdBatchFac());
    }
    private ScopedIdBatchType(ScopedIdBatchType parent) {
        super(parent);
    }

    /**
     * Get a {@link ScopedIdBatchType} that will create batches bound to a new
     * {@link Scope}. Such batch can hold terms from any scope, but {@code put*()} operations
     * will allocate new IDs on the scope allocated here.
     *
     * <p><strong>Important:</strong> The maximum number of concurrently existing scopes is
     * very small. The returned {@link ScopedIdBatchType} MUST be recycled to avoid
     * starvation of scopes</p>
     * @return a new {@link ScopedIdBatchType} bound to a scope.
     */
    public static Orphan<WithScope> beginScope() {
        return new WithScope(ScopedIds.allocScope()).sidecar;
    }

    @Override public boolean accepts(BatchType<?> other) {
        return other instanceof ScopedIdBatchType;
    }

    public static final class WithoutScope extends ScopedIdBatchType {
        public static final WithoutScope WITHOUT_SCOPE = new WithoutScope();
    }

    public static final class WithScope extends ScopedIdBatchType
                    implements SidecarOwned<WithScope> {
        public final Scope scope;
        private final SidecarScope sidecar;

        private static final class SidecarScope extends Sidecar<WithScope> {
            private SidecarScope(WithScope managed) {super(managed);}
            @Override public @Nullable Sidecar<WithScope> recycle(Object currentOwner) {
                internalMarkGarbage(currentOwner);
                Owned.safeRecycle(managed.scope, this);
                return null;
            }
        }

        private WithScope(Orphan<Scope> scope) {
            super(WithoutScope.WITHOUT_SCOPE);
            this.sidecar = new SidecarScope(this);
            // being owned by sidecar allows safely exposing scope since third parties
            // cannot recycle or steal it because they have no reference to sidecar
            this.scope   = scope.takeOwnership(sidecar);
        }
        @Override public Sidecar<WithScope> internalOwnedSidecar() {return sidecar;}

        @Override public <T extends BatchType<ScopedIdBatch>> T unscoped() {
            //noinspection unchecked
            return (T)WithoutScope.WITHOUT_SCOPE;
        }

        @Override public <T extends BatchType<ScopedIdBatch>> T reset(Object ownerIfOwned) {
            Owned.safeRecycle(this, ownerIfOwned);
            //noinspection unchecked
            return (T)beginScope().takeOwnership(ownerIfOwned);
        }

        @Override public String toString() {
            String cache = toStringCache;
            if (cache == null) {
                if (scope == null)
                    return "ScopedIdBatchType.WithScope";
                toStringCache = cache = "ScopedIdBatchType("+Integer.toHexString(scope.id())+")";
            }
            return cache;
        }

        @Override public String journalName() {return toString();}

        public Scope scope() {
            requireAlive();
            return scope;
        }
    }

    @Override public int hashId(long id) { return ScopedIds.hash(id); }

    @Override public boolean equals(long l, long r) { return ScopedIds.compare(l, r) == 0; }

    @Override public ByteSink<?, ?> appendNT(ByteSink<?, ?> sink, long id, byte[] nullValue) {
        return ScopedIds.writeNT(sink, id, nullValue);
    }
}
