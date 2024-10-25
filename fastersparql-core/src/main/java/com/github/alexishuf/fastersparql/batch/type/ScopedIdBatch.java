package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermView;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.EMPTY;
import static java.lang.Thread.currentThread;

public class ScopedIdBatch extends IdBatch<ScopedIdBatch> {
    ScopedIdBatch(long[] ids, short cols) {
        super(ids, cols, ScopedIdBatchType.WithoutScope.WITHOUT_SCOPE);
        BatchEvent.Created.record(this);
    }

    protected static final class Concrete extends ScopedIdBatch implements Orphan<ScopedIdBatch> {
        @SuppressWarnings("unused") // add 64 bytes of padding against false sharing
        private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
        public Concrete(long[] ids, short cols) {super(ids, cols);}
        @Override public ScopedIdBatch takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override protected void nodeCleanupBeforeRecycle() {
        type = null;
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public Orphan<ScopedIdBatch> dup() {return dup((int)currentThread().threadId());}
    @Override public Orphan<ScopedIdBatch> dup(int threadId) {
        ScopedIdBatch b = type.createForThread(threadId, cols).takeOwnership(this);
        b.copy(this);
        return b.releaseOwnership(this);
    }

    @Override public Orphan<ScopedIdBatch> dupRow(int row) {
        return dupRow(row, (int)currentThread().threadId());
    }
    @Override public Orphan<ScopedIdBatch> dupRow(int row, int threadId) {
        short cols = this.cols;
        var b = type.createForThread(threadId, cols).takeOwnership(this);
        b.doPut(this, row*cols, 0, (short)1, cols);
        return b.releaseOwnership(this);
    }

    /* --- --- --- term-level accessors --- --- --- */

    public int hash(int row, int col) { return ScopedIds.hash(id(row, col)); }

    @Override public int hash(int row) {
        if (row >= rows) throw new IndexOutOfBoundsException("row >= rows");
        int cols = this.cols, base = row*cols, acc = 0;
        for (int c = 0; c < cols; c++)
            acc ^= ScopedIds.hash(arr[base+c]);
        return acc;
    }

    @Override public boolean equals(@NonNegative int row, long[] ids, int idsOffset) {
        short rows = this.rows, cols = this.cols;
        //noinspection ConstantValue
        if (row < 0 || row >= rows)
            throw new IndexOutOfBoundsException(row);
        for (int i = 0, offset = row*cols; i < cols; i++) {
            if (!ScopedIds.equals(arr[offset+i], ids[idsOffset+i])) return false;
        }
        return true;
    }
    @Override public boolean equals(@NonNegative int row, @NonNegative int col, long rId) {
        return ScopedIds.equals(id(row, col), rId);
    }

    @Override public @Nullable FinalTerm get(@NonNegative int row, @NonNegative int col) {
        requireAlive();
        //noinspection ConstantValue
        if (row < 0 || col < 0 || row >= rows || col >= cols)
            throw new IndexOutOfBoundsException();
        int address = row*cols + col;
        FinalTerm term = cachedTerm(address);
        if (term == null && (term=ScopedIds.asTerm(arr[address])) != null)
            cacheTerm(address, term);
        return term;
    }

    @Override public TermInfo.Type get(@NonNegative int row, @NonNegative int col, TermInfo info) {
        return ScopedIds.info(id(row, col), info);
    }

    @Override public boolean getView(@NonNegative int row, @NonNegative int col, TermView dest) {
        return ScopedIds.view(id(row, col), dest);
    }

    @Override
    public boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest) {
        return ScopedIds.view(id(row, col), dest);
    }

    @Override public int len(@NonNegative int row, @NonNegative int col) {
        return ScopedIds.len(id(row, col));
    }

    @Override
    public boolean localView(@NonNegative int row, @NonNegative int col, SegmentRopeView dest) {
        return ScopedIds.localView(id(row, col), dest);
    }

    @Override public int localLen(@NonNegative int row, @NonNegative int col) {
        return ScopedIds.localLen(id(row, col));
    }

    @Override public @NonNull FinalSegmentRope shared(@NonNegative int row, @NonNegative int col) {
        return ScopedIds.shared(id(row, col));
    }

    @Override public boolean sharedSuffixed(@NonNegative int row, @NonNegative int col) {
        return ScopedIds.sharedSuffix(id(row, col));
    }

    @Override public int lexEnd(@NonNegative int row, @NonNegative int col) {
        return ScopedIds.lexEnd(id(row, col));
    }

    @Override public Term.@Nullable Type termType(int row, int col) {
        return ScopedIds.termType(id(row, col));
    }

    @Override
    public int writeSparql(ByteSink<?, ?> dest, int row, int column, PrefixAssigner prefixAssigner) {
        return ScopedIds.writeSparql(dest, id(row, column), prefixAssigner);
    }

    @Override public void write(ByteSink<?, ?> dest, int row, int col, int begin, int end) {
        ScopedIds.write(dest, id(row, col), begin, end);
    }

    /* --- --- --- mutators --- --- --- */


    @Override public void putTerm(int destCol, Term t) {
        if (!(type instanceof ScopedIdBatchType.WithScope scoped))
            throw new IllegalStateException("No scope attached to this batch");
        putTerm(destCol, scoped.scope.put(t));
    }

    @Override public void putNullTerm(int col) {
        putTerm(col, 0);
    }

    @Override
    public void putTerm(int col, FinalSegmentRope shared, MemorySegment local,
                        byte @Nullable [] localU8, long localOff, int localLen,
                        byte sharedKind) {
        if (!(type instanceof ScopedIdBatchType.WithScope scoped))
            throw new IllegalStateException("No scope attached to this batch");
        putTerm(col, scoped.scope.put(shared, local, localU8, localOff, localLen, sharedKind));
    }

    @Override
    public void putTermLocalByReference(int col, FinalSegmentRope shared, MemorySegment local,
                                        byte @Nullable [] localU8, long localOff, int localLen,
                                        byte sharedKind) {
        if (!(type instanceof ScopedIdBatchType.WithScope scoped))
            throw new IllegalStateException("No scope attached to this batch");
        putTerm(col, scoped.scope.put(shared, local, localU8, localOff, localLen, sharedKind));
    }

    @Override
    public void putTerm(int col, FinalSegmentRope shared, PlainRope local, int localOff,
                        int localLen, byte sharedKind) {
        if (!(type instanceof ScopedIdBatchType.WithScope scoped))
            throw new IllegalStateException("No scope attached to this batch");
        PooledMutableRope pmr;
        SegmentRope sr;
        if (local instanceof SegmentRope r) {
            sr = r;
            pmr = null;
        } else {
            sr = pmr = PooledMutableRope.getWithCapacity(localLen);
        }
        try {
            putTerm(col, scoped.scope.put(shared, sr.segment, sr.utf8, sr.offset+localOff,
                    localLen, sharedKind));
        } finally {
            if (pmr != null)
                pmr.close();
        }
    }

    @Override protected void putUninternable(int destCol, TermInfo t) {
        assert t.type == TermInfo.Type.UNINTERNABLE;
        if (!(type instanceof ScopedIdBatchType.WithScope scoped))
            throw new IllegalStateException("No scope attached to this batch");
        var s = scoped.scope;
        try (var r = PooledMutableRope.getWithCapacity(t.localLen + t.sharedLen)) {
            if (SharedKind.isPrefix(t.sharedKind))
                r.append(t.shared);
            r.append(t.localSeg, t.localU8, t.localOff, t.localLen);
            if (SharedKind.isSuffix(t.sharedKind))
                r.append(t.shared);
            putTerm(destCol, s.put(EMPTY, r.segment, r.utf8, r.offset, r.len,
                                   SharedKind.whole(SharedKind.isLit(t.sharedKind))));
        }
    }

}
