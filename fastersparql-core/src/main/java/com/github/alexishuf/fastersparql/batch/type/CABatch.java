package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.sparql.expr.PooledTermView;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermView;
import com.github.alexishuf.fastersparql.util.LowLevelHelper;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.List;

import static com.github.alexishuf.fastersparql.batch.type.BatchType.PREFERRED_BATCH_TERMS;
import static com.github.alexishuf.fastersparql.batch.type.CABatchType.CA;
import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.*;
import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.Rope.FNV_BASIS;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.lang.Thread.currentThread;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public sealed class CABatch extends Batch<CABatch> {
    public static final int BYTES = 16 /* header */
                                  + 2*2 /* rows, cols*/
                                  +   4 /* tail */
                                  +   4 /* padding */
                                  + 2*4 /* arrays */
                                  + 4*2 /* shorts */;

    private final Object[] objs;
    private final long[] md;
    private short dirtyTerms;
    private short offerRowBase;
    private short rowsCapacity;
    private final short termsCapacity;


    /*  --- --- --- helpers --- --- ---  */

    private static final int SUF_MASK = 0x80000000;
    private static final int LEN_MASK = 0x7fffffff;
    private static final int SHR_IDX = 0;
    private static final int SEG_IDX = 1;
    private static final int UTF_IDX = 2;

    private boolean isSuff(int termIdx) { return      (md[ termIdx<<1   ] & 0x80000000L) != 0; }
    private int    flagLen(int termIdx) { return (int)(md[ termIdx<<1   ] & 0xffffffffL); }
    private int       len (int termIdx) { return (int)(md[ termIdx<<1   ] & 0x7fffffffL); }
    private int     mdHash(int termIdx) { return (int)(md[ termIdx<<1   ]>>>32); }
    private long       off(int termIdx) { return       md[(termIdx<<1)+1]; }

    private  FinalSegmentRope  sh(int ti3) { return (FinalSegmentRope)objs[ti3+SHR_IDX]; }
    private  MemorySegment    seg(int ti3) { return    (MemorySegment)objs[ti3+SEG_IDX]; }
    private  byte[]          utf8(int ti3) { return           (byte[])objs[ti3+UTF_IDX]; }

    private void setHash(int termIdx, int hash) {
        int i = termIdx<<1;  //          +---> undo sign extension
        md[i] = ((long)hash<<32) | (md[i]&0xffffffffL);
    }
    private void setNull(int termIdx) {
        int oi = termIdx*3, mi = termIdx<<1;
        objs[oi  ] = null;
        objs[oi+1] = null;
        objs[oi+2] = null;
        md  [mi  ] = 0L;
        md  [mi+1] = 0L;
    }
    private void setTerm(int termIdx, @Nullable FinalSegmentRope sh,
                         MemorySegment localSeg, byte @Nullable[] localU8,
                         long offset, int len, boolean suffixShared, int hash) {
        long md0 = ((long)hash<<32) | (len&0x7fffffffL);
        if (suffixShared) {
            md0 |= 0x80000000L;
            if (sh == EMPTY)
                sh = null;
        }
        int oi = termIdx*3, mi = termIdx<<1;
        md  [mi  ] = md0;
        md  [mi+1] = offset;
        objs[oi+SHR_IDX] = sh;
        objs[oi+SEG_IDX] = localSeg;
        objs[oi+UTF_IDX] = localU8;
    }
    private void copyTerm(int dstTerm, CABatch o, int srcTerm) {
        int s = srcTerm*3, d = dstTerm*3;
        objs[d  ] = o.objs[s  ];
        objs[d+1] = o.objs[s+1];
        objs[d+2] = o.objs[s+2];
        md[d=dstTerm<<1] = o.md[s=srcTerm<<1];
        md[d+1         ] = o.md[s+1         ];
    }

    private void copy0(CABatch o, int srcTerm, int dstTerm, int nTerms) {
        arraycopy(o.objs, srcTerm*3, objs, dstTerm*3,  nTerms*3);
        arraycopy(o.md,   srcTerm<<1, md,  dstTerm<<1, nTerms<<1);
    }
    private void setRows(int rows) {
        this.dirtyTerms = (short)Math.max(dirtyTerms, rows*cols);
        this.rows       = (short)rows;
    }
    private void putRows0(CABatch o, int srcRow, int nRows) {
        final short rows = this.rows, cols = this.cols;
        setRows(rows+nRows);
        int nTerms = nRows*cols, srcTerm = srcRow*cols, dstTerm = rows*cols;
        arraycopy(o.objs, srcTerm*3,  objs, dstTerm*3,  nTerms*3);
        arraycopy(o.md,   srcTerm<<1, md,   dstTerm<<1, nTerms<<1);
    }

    private int termIdx(int row) {
        requireAlive();
        if (row < 0 || row >= rows)
            throw new IndexOutOfBoundsException(mkOutOfBoundsMsg(row, 0));
        return row*cols;
    }

    private int termIdx(int row, int col) {
        requireAlive();
        final short cols = this.cols;
        if ((row|col) < 0 || row >= rows || col > cols)
            throw new IndexOutOfBoundsException(mkOutOfBoundsMsg(row, col));
        return row*cols + col;
    }

    private CABatch createTail() {return setTail(CA.create(cols));}

    @Override protected boolean validateNode(Validation validation) {
        if (!SELF_VALIDATE || validation == Validation.NONE)
            return true;
        if (termsCapacity > md.length>>1)
            return false;
        if (termsCapacity > objs.length/3)
            return false;
        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                int ti = termIdx(r, c), ti3 = ti*3, len = len(ti);
                long off = off(ti);
                if (off < 0)
                    return false; // bad offset
                var sh = sh(ti3);
                var seg =  seg(ti3);
                var u8  = utf8(ti3);
                if (seg == null && len != 0)
                    return false; // missing MemorySegment
                if (u8 != null && seg == null)
                    return false; // has byte[] but not MemorySegment
                if (seg != null) {
                    if (seg.heapBase().orElse(null) != u8)
                        return false; // segment does not wrap utf8
                    if (seg.byteSize() < off+len)
                        return false; // off+len is out of bounds
                }
                if (isSuff(ti)) {
                    if (seg == null)
                        return false; // no local with shared suffix
                    if (seg.get(JAVA_BYTE, off) != '"')
                        return false; // shared suffix, but not a literal
                    else if (sh != null && sh.get(0) != '"')
                        return false; // unclosed literal
                }
            }
        }
        return super.validateNode(validation);
    }
    /* --- --- --- Lifecycle --- --- --- */

    public static Orphan<CABatch> createNotPooled(int rowsCapacity, int cols) {
        return new Concrete(rowsCapacity, cols);
    }

    private CABatch(int rowsCapacity, int cols) {
        super((short)0, (short)cols);
        int terms = rowsCapacity*cols;
        if (terms > Short.MAX_VALUE)
            throw new IllegalArgumentException("More than 32768 terms");
        this.md   = new   long[terms*2];
        this.objs = new Object[terms*3];
        this.termsCapacity = (short)terms;
        this.rowsCapacity = (short)rowsCapacity;
        updateLeakDetectorRefCapacity();
        BatchEvent.Created.record(this);
    }

    static final class Concrete extends CABatch implements Orphan<CABatch> {
        @SuppressWarnings("unused") // add 64 bytes of padding against false sharing
        private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
        public Concrete(int rowsCapacity, int cols) {super(rowsCapacity, cols);}
        @Override public CABatch takeOwnership(Object newOwner) {
            return takeOwnership0(newOwner);
        }
    }

    @Override public @Nullable CABatch recycle(Object currentOwner) {
        Object nodeOwner = currentOwner;
        for (CABatch node = this, next; node != null; nodeOwner=node, node=next) {
            next = node.next;
            node.next = null;
            node.tail = node;
            if (node.termsCapacity != PREFERRED_BATCH_TERMS)
                node.safeMarkGarbage(nodeOwner);
            else if (node.dirtyTerms < 8)
                node.dropRefsAndOffer(nodeOwner);
            else
                CABatchCleaner.INSTANCE.sched(node, nodeOwner);
        }
        return null;
    }
    void clearAndMarkRecycled(Object currentOwner) {
        clear();
        BatchEvent.Pooled.record(this);
        internalMarkRecycled(currentOwner);
    }
    private void dropRefsAndOffer(Object currentOwner) {
        internalMarkRecycled(currentOwner);
        currentOwner = RECYCLED;
        Arrays.fill(objs, 0, dirtyTerms*3, null);
        clear();
        BatchEvent.Pooled.record(this);
        if (CA.pool.offer(this) != null)
            safeMarkGarbage(currentOwner);
    }
    private void safeMarkGarbage(Object currentOwner) {
        try {
            internalMarkGarbage(currentOwner);
        } catch (Throwable ignored) {
            assert false : "markGarbage() failed";
        }
    }
    void completeAsyncRecycle(Object currentOwner) {
        if (termsCapacity == PREFERRED_BATCH_TERMS)
            dropRefsAndOffer(currentOwner);
        else
            safeMarkGarbage(currentOwner);
    }

    /* --- --- --- Batch mutators --- --- --- */

    @Override public void clear() {
        rows = 0;
        tail = this;
        if (next != null)
            next = next.recycle(this);
    }

    @Override public CABatch clear(int newColumns) {
        if (newColumns > termsCapacity)
            throw new IllegalArgumentException("Too wide");
        rows         = 0;
        cols         = (short)newColumns;
        rowsCapacity = cols == 0 ? Short.MAX_VALUE : (short)(termsCapacity/cols);
        tail         = this;
        if (next != null)
            next = next.recycle(this);
        return this;
    }

    void garbageFillUntil(int goalRows) {
        if (goalRows*cols > termsCapacity)
            throw new IllegalArgumentException("goalRows overflows batch capacity");
        if (goalRows < rows)
            return;
        setRows(goalRows);
    }

    @Override public void copy(CABatch o) {
        short cols = this.cols;
        if (o.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        if (cols == 0) {
            addRowsToZeroColumns(o.totalRows());
        } else {
            var tail = tail();
            for (; o != null; o = o.next) {
                o.requireAlive();
                for (short srcRow = 0, nRows, oRows=o.rows; srcRow < oRows; srcRow += nRows) {
                    nRows = (short)Math.min(oRows-srcRow, tail.rowsCapacity-tail.rows);
                    if (nRows == 0)
                        tail = createTail();
                    else
                        tail.putRows0(o, srcRow, nRows);
                }
            }
        }
    }

    @Override public void append(Orphan<CABatch> orphan) {
        CABatch o, tail;
        try {
            tail = tail();
        } catch (Throwable t) { Orphan.safeRecycle(orphan); throw t; }
        o = orphan.takeOwnership(tail);
        try {
            short cols = this.cols, oRows;
            if (o.cols != cols)
                throw new IllegalArgumentException("cols mismatch");
            if (cols == 0) {
                addRowsToZeroColumns(o.totalRows());
                o = o.recycle(tail);
            } else {
                while (o != null) {
                    if (tail.rows+(oRows=o.rows) <= tail.rowsCapacity) {
                        tail.putRows0(o, 0, oRows);
                        o = o.dropHead(tail);
                    } else {
                        o = setTailAndReturnNull(o);
                    }
                }
            }
        } finally {
            if (o != null) Batch.safeRecycle(o, this);
        }
    }

    @Override public void deFragmentMiddleNodes() {
        CABatch prev = next, tail = this.tail, n;
        if (prev == null || (n=prev.next) == tail || n == null)
            return;
        while ((n=prev.next) != tail && n != null) {
            short nRows = n.rows;
            if (prev.rows+nRows <= prev.rowsCapacity) {
                prev.putRows0(n, 0, nRows);
                prev.next = n.dropHead(prev);
            } else {
                prev = n;
            }
        }
    }

    @Override public void putConverting(Batch<?> other) {
        if (other instanceof CABatch ca) {
            copy(ca);
        } else {
            try (var lv = PooledSegmentRopeView.ofEmpty()) {
                for (; other != null; other = other.next) {
                    for (short r = 0, oRows = other.rows; r < oRows; r++)
                        putRowConverting0(other, r, lv);
                }
            }
        }
    }

    /* --- --- --- row mutators --- --- --- */

    private void putRowConverting0(Batch<?> other, int r, PooledSegmentRopeView lv) {
        beginPut();
        for (short c = 0; c < cols; c++) {
            if (other.localView(r, c, lv)) {
                var sh = other.shared(r, c);
                boolean suffixed = (sh.len == 0 ? lv : sh).get(0) == '"';
                putTerm(c, sh, lv.segment, lv.utf8, lv.offset, lv.len, suffixed);
            }
        }
        commitPut();
    }

    @Override public void putRowConverting(Batch<?> other, int row) {
        try (var lv = PooledSegmentRopeView.ofEmpty()) {
            putRowConverting0(other, row, lv);
        }
    }

    @Override public void beginPut() {
        var tail = tail();
        short cols = tail.cols, begin = (short)(tail.rows*cols);
        int end = begin+cols;
        if (end > tail.termsCapacity) {
            tail = createTail();
            begin = 0;
            end = cols;
        }
        Arrays.fill(tail.md,   begin<<1, end<<1, 0L);
        Arrays.fill(tail.objs, begin*3,  end*3,  null);
        tail.offerRowBase = begin;
    }

    @Override public void commitPut() {
        var tail = tail();
        if (tail.offerRowBase < 0)
            throw new IllegalStateException("no active beginPut()");
        tail.setRows(tail.rows+1);
        tail.offerRowBase = -1;
        assert validate();
    }

    @Override public void abortPut() throws IllegalStateException {
        var tail = tail();
        tail.requireAlive();
        if (tail.offerRowBase < 0) return;
        tail.offerRowBase = -1;
        dropEmptyTail();
        assert validate();
    }

    private CABatch tailForPutTerm(int col) {
        var tail = SELF_VALIDATE ? tail() : this.tail;
        if (tail.offerRowBase < 0)
            throw new IllegalStateException("No active beginPut()");
        if (col < 0 || col >= cols)
            throw new IndexOutOfBoundsException("col not in [0, cols)");
        return tail;
    }

    @Override public void putTerm(int col, Term t) {
        var tail = tailForPutTerm(col);
        int dstTerm = tail.offerRowBase + col;
        if (t == null) {
            tail.setNull(dstTerm);
        } else {
            MemorySegment     lSeg;
            byte[]             lU8;
            long              lOff;
            SegmentRope      local = t.local();
            int               lLen = local.len;
            NakedRopeFactory naked;
            if (local instanceof FinalSegmentRope) {
                naked = null;
                lSeg  = local.segment;
                lU8   = local.utf8;
                lOff  = local.offset;
            } else {
                naked = RopeFactory.make(local.len).add(local).naked();
                lSeg  = naked.segment();
                lU8   = naked.utf8();
                lOff  = naked.begin();
            }
            tail.setTerm(dstTerm, FinalSegmentRope.asFinal(t.shared()),
                         lSeg, lU8, lOff, lLen, t.sharedSuffixed(), t.cachedHash());
            if (naked != null)
                naked.close();
        }
    }

    @Override public void putTerm(int destCol, CABatch batch, int row, int col) {
        var tail = tailForPutTerm(col);
        tail.copyTerm(tail.offerRowBase+destCol, batch, row*batch.cols+col);
    }

    @Override
    public void putTerm(int col, FinalSegmentRope shared, MemorySegment local,
                        byte @Nullable [] localU8, long localOff, int localLen,
                        boolean sharedSuffix) {
        var tail = tailForPutTerm(col);
        var nkd = RopeFactory.make(localLen).add(local, localU8, localOff, localLen).naked();
        tail.setTerm(tail.offerRowBase + col, shared,
                      nkd.segment(), nkd.utf8(),
                      nkd.begin(), nkd.len(), sharedSuffix, 0);
        nkd.close();
    }

    @Override
    public void putTerm(int col, FinalSegmentRope shared, PlainRope local, int localOff,
                        int localLen, boolean sharedSuffix) {
        if (local instanceof FinalSegmentRope f) {
            putTermLocalByReference(col, shared, f.segment, f.utf8, f.offset, f.len, sharedSuffix);
        } else {
            var tail = tailForPutTerm(col);
            var nkd = RopeFactory.make(localLen).add(local, localOff, localLen).naked();
            tail.setTerm(tail.offerRowBase+col, shared,
                         nkd.segment(), nkd.utf8(), nkd.begin(), nkd.len(),
                         sharedSuffix, 0);
            nkd.close();
        }
    }

    @Override
    public void putTermLocalByReference(int col, FinalSegmentRope shared, MemorySegment local, byte @Nullable [] localU8, long localOff, int localLen, boolean sharedSuffix) {
        var tail = tailForPutTerm(col);
        tail.setTerm(tail.offerRowBase+col, shared,
                     local, localU8, localOff, localLen, sharedSuffix, 0);
    }

    @Override public void putNullTerm(int col) {
        var tail = tailForPutTerm(col);
        tail.setNull(offerRowBase+col);
    }

    @Override public void putRow(CABatch other, int row) {
        if (other.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        var tail = tail();
        if (tail.rows >= tail.rowsCapacity)
            tail = createTail();
        tail.putRows0(other, row, 1);
    }

    public void setRow(int dstRow, CABatch src, int srcRow) {
        int di = termIdx(dstRow), si = src.termIdx(srcRow);
        if (src.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        copy0(src, si, di, cols);
    }

    /* --- --- --- Batch accessors --- --- --- */

    @Override public BatchType<CABatch> type() {return CA;}

    @Override public Orphan<CABatch> dup() { return dup((int)currentThread().threadId()); }

    @Override public Orphan<CABatch> dup(int threadId) {
        var b = CA.createForThread(threadId, cols);
        ((CABatch)b).copy(this);
        return b;
    }

    @Override public int  rowsCapacity() {
        return cols==0 ? Short.MAX_VALUE : (short)(termsCapacity/cols);
    }
    @Override public int termsCapacity() { return termsCapacity;}

    @Override public int totalBytesCapacity() { return termsCapacity*12; }

    @Override public boolean hasCapacity(int terms, int localBytes) {
        return terms <= termsCapacity;
    }

    /* --- --- --- Row accessors --- --- --- */

    @Override public Orphan<CABatch> dupRow(int row) {
        return dupRow(row, (int)currentThread().threadId());
    }

    @Override public Orphan<CABatch> dupRow(int row, int threadId) {
        requireAlive();
        if (row < 0 || row >= rows)
            throw new IndexOutOfBoundsException("row out of bounds");
        var b = CA.createForThread(threadId, cols).takeOwnership(this);
        b.putRows0(this, row, 1);
        return b.releaseOwnership(this);
    }

    @Override public boolean equals(int row, CABatch other, int oRow) {
        short cols = this.cols;
        if (cols != other.cols) return false;
        int lti = termIdx(row), rti = other.termIdx(oRow);
        try (var lv = PooledTermView.ofEmptyString();
             var rv = PooledTermView.ofEmptyString()) {
            for (int c = 0; c < cols; c++) {
                if (!equals(lv, lti+c, rv, other, rti+c))
                    return false;
            }
            return true;
        }
    }

    @Override public boolean equals(int row, Term[] other) {
        short cols = this.cols;
        if (other.length != cols) return false;
        int ti = termIdx(row);
        try (var lv = PooledTermView.ofEmptyString()) {
            for (short c = 0; c < cols; c++) {
                boolean present = getView0(ti+c, lv);
                var o = other[c];
                if (present ^ (o != null) || (present && !lv.equals(o)))
                    return false;
            }
            return true;
        }
    }

    @Override public boolean equals(int row, List<Term> other) {
        short cols = this.cols;
        if (other.size() != cols) return false;
        int ti = termIdx(row);
        try (var lv = PooledTermView.ofEmptyString()) {
            boolean ok = true;
            for (short c = 0; ok && c < cols; c++) {
                boolean present = getView0(ti+c, lv);
                var o = other.get(c);
                ok = present == (o == null) || present && lv.equals(o);
            }
            return ok;
        }
    }

    /* --- --- --- Term accessors --- --- --- */

    @Override public @Nullable FinalTerm get(@NonNegative int row, @NonNegative int col) {
        final int ti = termIdx(row, col), ti3 = ti*3;
        FinalSegmentRope sh = sh(ti3);
        MemorySegment seg   = seg(ti3);
        int len             = len(ti);
        if (len == 0 && (sh == null || sh.len == 0))
            return null;
        var local = len == 0 ? EMPTY : new FinalSegmentRope(seg, utf8(ti3), off(ti), len);
        return new FinalTerm(sh, local, isSuff(ti));
    }

    @Override public boolean getView(@NonNegative int row, @NonNegative int col, TermView dest) {
        return getView0(termIdx(row, col), dest);
    }
    private boolean getView0(int ti, TermView dest) {
        final int ti3 = ti*3;
        var sh = sh(ti3);
        int lLen = len(ti);
        if (lLen == 0 && (sh == null || sh.len == 0))
            return false;
        if (sh == null)
            sh = EMPTY;
        dest.wrap(sh, seg(ti3), utf8(ti3), off(ti), lLen, isSuff(ti));
        return true;
    }

    @Override
    public boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest) {
        final int ti = termIdx(row, col), ti3 = ti*3;
        var sh = sh(ti3);
        int lLen = len(ti);
        if (lLen == 0 && (sh == null || sh.len == 0))
            return false;
        dest.wrapFirst(sh == null ? EMPTY : sh);
        dest.wrapSecond(seg(ti3), utf8(ti3), off(ti), lLen);
        if (isSuff(ti))
            dest.flipSegments();
        return true;
    }

    @Override
    public boolean localView(@NonNegative int row, @NonNegative int col, SegmentRopeView dest) {
        final int ti = termIdx(row, col), ti3 = ti*3;
        var sh = sh(ti3);
        int lLen = len(ti);
        if (lLen == 0 && (sh == null || sh.len == 0))
            return false;
        dest.wrap(seg(ti3), utf8(ti3), off(ti), lLen);
        return true;
    }

    @Override public @NonNull FinalSegmentRope shared(@NonNegative int row, @NonNegative int col) {
        var sh = sh(termIdx(row, col)*3);
        return sh == null ? EMPTY : sh;
    }

    @Override public boolean sharedSuffixed(@NonNegative int row, @NonNegative int col) {
        return isSuff(termIdx(row, col));
    }

    @Override public int len(@NonNegative int row, @NonNegative int col) {
        int ti = termIdx(row, col);
        var sh = sh(ti*3);
        return (sh == null ? 0 : sh.len) + len(ti);
    }

    @Override public int lexEnd(@NonNegative int row, @NonNegative int col) {
        final int ti = termIdx(row, col), ti3 = ti*3;
        int lLen = flagLen(ti);
        long lOff = off(ti);
        if (lLen == 0) {
            return 0;
        } else if (lLen < 0) {
            lLen &= LEN_MASK;
            if (objs[ti3+SHR_IDX] != null)
                return lLen;
        }
        var localSeg =  seg(ti3);
        var localU8  = utf8(ti3);
        if (localSeg == null || localSeg.get(JAVA_BYTE, lOff) != '"')
            return 0;
        try (var view = PooledSegmentRopeView.of(localSeg, localU8, lOff, lLen)) {
            return view.skipUntilLastNear(0, lLen, (byte)'"');
        }
    }

    @Override public int localLen(@NonNegative int row, @NonNegative int col) {
        return len(termIdx(row, col));
    }

    @Override public int uncheckedLocalLen(@NonNegative int row, @NonNegative int col) {
        return len(row*cols+col);
    }

    @Override public Term.@Nullable Type termType(int row, int col) {
        int ti = termIdx(row, col), ti3 = ti*3;
        int lLen = flagLen(ti);
        if (lLen < 0)
            return Term.Type.LIT; // suffixed
        byte first;
        var sh = sh(ti3);
        if (sh == null || sh.len == 0) {
            if (lLen == 0) return null; // no term
            var lSeg = seg(ti3);
            first = lSeg.get(JAVA_BYTE, off(ti));
        } else {
            first = sh.get(0);
        }
        return switch (first) {
            case '"'      -> Term.Type.LIT;
            case '_'      -> Term.Type.BLANK;
            case '<'      -> Term.Type.IRI;
            case '?', '$' -> Term.Type.VAR;
            default       -> throw new IllegalStateException();
        };
    }

    @Override
    public int writeSparql(ByteSink<?, ?> dest, int row, int column,
                           PrefixAssigner prefixAssigner) {
        final int ti = termIdx(row, column), ti3 = ti*3;
        var sh   =   sh(ti3);
        var lSeg =  seg(ti3);
        var lU8  = utf8(ti3);
        int len  = len(ti);
        if (len != 0 || (sh != null && sh.len != 0))
            return Term.toSparql(dest, prefixAssigner, sh, lSeg, lU8, off(ti), len, isSuff(ti));
        return 0;
    }

    @Override public void writeNT(ByteSink<?, ?> dest, int row, int col) {
        final int ti = termIdx(row, col), ti3 = ti*3;
        var sh   =   sh(ti3);
        var lSeg =  seg(ti3);
        var lU8  = utf8(ti3);
        int  len = len(ti);
        long off = off(ti);
        if (isSuff(ti)) {
            if (len > 0    ) dest.append(lSeg, lU8, off, len);
            if (sh != null ) dest.append(sh);
        } else {
            if (sh != null) dest.append(sh);
            if (len >  0  ) dest.append(lSeg, lU8, off, len);
        }
    }

    @Override public void write(ByteSink<?, ?> dest, int row, int col, int begin, int end) {
        try (var view = PooledTwoSegmentRope.ofEmpty()) {
            if (getRopeView(row, col, view))
                dest.append(view, begin, end);
        }
    }

    @Override public int hash(int row, int col) {
        final int ti = termIdx(row, col);
        int h = mdHash(ti);
        if (h != 0)
            return h; // cache hit
        h = LowLevelHelper.U == null ? computeHashSafe(ti) : computeHashUnsafe(ti);
        setHash(ti, h);
        return h;
    }

    private int hashTerm(int ti) {
        try (var view = PooledTermView.ofEmptyString()) {
            return getView0(ti, view) ? view.hashCode() : FNV_BASIS;
        }
    }

    private int computeHashUnsafe(int ti) {
        int ti3 = ti*3;
        var sh   = sh(ti3);
        if (Term.isNumericDatatype(sh))
            return hashTerm(ti);
        var lSeg = seg(ti3);
        var lU8  = utf8(ti3);
        int  fstLen, sndLen = flagLen(ti);
        long fstOff, sndOff = (lSeg == null ? 0 : lSeg.address()) + off(ti);

        if (sh == null)
            return FinalSegmentRope.hashUnsafe(FNV_BASIS, lU8, sndOff, sndLen&LEN_MASK);
        byte[] fst, snd;
        long shOff = sh.segment.address() + sh.offset;
        if ((sndLen&SUF_MASK) == 0) {
            fst = sh.utf8; fstOff = shOff; fstLen = sh.len;
            snd = lU8;                     sndLen &= LEN_MASK;
        } else {
            fst =  lU8;    fstOff = sndOff; fstLen = sndLen&LEN_MASK;
            snd = sh.utf8; sndOff =  shOff; sndLen = sh.len;
        }
        int h = FinalSegmentRope.hashUnsafe(FNV_BASIS, fst, fstOff, fstLen);
        return  FinalSegmentRope.hashUnsafe(h,         snd, sndOff, sndLen);
    }

    private int computeHashSafe(int ti) {
        int ti3 = ti*3;
        var sh   = sh(ti3);
        if (Term.isNumericDatatype(sh))
            return hashTerm(ti);
        var lSeg = seg(ti3);
        int  fstLen, sndLen = flagLen(ti);
        long fstOff, sndOff = off(ti);

        if (sh == null)
            return FinalSegmentRope.hashSafe(FNV_BASIS, lSeg, sndOff, sndLen&LEN_MASK);
        MemorySegment fst, snd;
        if ((sndLen&SUF_MASK) == 0) {
            fst = sh.segment; fstOff = sh.offset;  fstLen = sh.len;
            snd = lSeg;                           sndLen &= LEN_MASK;
        } else {
            fst = lSeg;        fstOff = sndOff;     fstLen = sndLen&LEN_MASK;
            snd = sh.segment;  sndOff = sh.offset;  sndLen = sh.len;
        }
        int h = FinalSegmentRope.hashSafe(FNV_BASIS, fst, fstOff, fstLen);
        return  FinalSegmentRope.hashSafe(h,         snd, sndOff, sndLen);
    }

    private boolean equals(TermView myView, int myTermIdx,
                           TermView otherView, CABatch other, int otherTermIdx) {
        boolean present = getView0(myTermIdx, myView);
        if (present != other.getView0(otherTermIdx, otherView))
            return false; // "is present" differs
        return !present || myView.equals(otherView);
    }

    @Override
    public boolean equals(@NonNegative int row, @NonNegative int col, @Nullable Term other) {
        int ti = termIdx(row, col);
        try (var view = PooledTermView.ofEmptyString()) {
            if (getView0(ti, view))
                return view.equals(other);
            else
                return other == null;
        }
    }

    @Override
    public boolean equals(@NonNegative int row, @NonNegative int col,
                          CABatch other, int oRow, int oCol) {
        try (var lv = PooledTermView.ofEmptyString();
             var rv = PooledTermView.ofEmptyString()) {
            return equals(lv, termIdx(row, col), rv, other, other.termIdx(oRow, oCol));
        }
    }

    /* --- --- --- operation objects --- --- --- */

    public static abstract sealed class Merger extends BatchMerger<CABatch, Merger> {
        private final short outColumns;
        public Merger(BatchType<CABatch> batchType, Vars outVars, short[] sources) {
            super(batchType, outVars, sources);
            outColumns = (short)sources.length;
        }

        static final class Concrete extends Merger implements Orphan<Merger> {
            public Concrete(BatchType<CABatch> batchType, Vars outVars, short[] sources) {
                super(batchType, outVars, sources);
            }
            @Override public Merger takeOwnership(Object o) {return takeOwnership0(o);}
        }


        private CABatch setupDst(Orphan<CABatch> offer, boolean inPlace) {
            int cols = outColumns;
            if (offer != null) {
                CABatch b = offer.takeOwnership(this);
                if (b.rows == 0 || inPlace)
                    b.cols = (short)cols;
                else if (b.cols != cols)
                    throw new IllegalArgumentException("dst.cols != outColumns");
                return b;
            }
            return CA.create(cols).takeOwnership(this);
        }

        private CABatch createTail(CABatch root) {
            return root.setTail(CA.create(outColumns));
        }

        protected Orphan<CABatch> mergeWithMissing(CABatch dst, CABatch left, int leftRow,
                                                   CABatch right) {
            int l = leftRow*left.cols;
            CABatch tail = dst.tail();
            for (int rows = right == null || right.rows == 0 ? 1 : right.totalRows(), nr
                 ; rows > 0; rows -= nr) {
                int d = tail.rows*tail.cols;
                if ((nr=(tail.termsCapacity-d)/sources.length) <= 0) {
                    nr = (tail=createTail(dst)).termsCapacity/sources.length;
                    d = 0;
                }
                tail.rows += (short)(nr=min(nr, rows));
                for (int e = d+nr*sources.length; d < e; d += sources.length) {
                    for (int c = 0, s; c < sources.length; c++) {
                        if ((s=sources[c]) > 0)
                            tail.copyTerm(d+c, left, l+s-1);
                        else
                            tail.setNull(d+c);
                    }
                }
            }
            assert tail.validate();
            return dst.releaseOwnership(this);
        }

        @Override public final Orphan<CABatch>
        merge(@Nullable Orphan<CABatch> dstOffer, CABatch left, int leftRow,
              @Nullable CABatch right) {
            var dst = setupDst(dstOffer, false);
            if (sources.length == 0)
                return mergeThin(dst, right).releaseOwnership(this);
            if (right == null || right.rows*right.cols == 0)
                return mergeWithMissing(dst, left, leftRow, right);

            short l = (short)(leftRow*left.cols), rc = right.cols;
            CABatch tail = dst.tail();
            for (; right != null; right = right.next) {
                for (short rr = 0, rRows = right.rows, nr; rr < rRows; rr += nr) {
                    if ((nr=(short)(tail.termsCapacity/sources.length-tail.rows)) <= 0) {
                        tail = createTail(dst);
                        nr = (short)(tail.termsCapacity/sources.length);
                    }
                    nr = (short)min(nr, rRows-rr);
                    short d = (short)(tail.rows*tail.cols);
                    tail.rows += nr;
                    for (short r = (short)(rr*rc), re = (short)((rr+nr)*rc); r < re; r+=rc) {
                        for (int c = 0, s; c < sources.length; c++, ++d) {
                            if      ((s = sources[c]) == 0) tail.setNull(d);
                            else if (s > 0)                 tail.copyTerm(d, left,  l+s-1);
                            else                            tail.copyTerm(d, right, r-s-1);
                        }
                    }
                }
            }
            assert dst.validate();
            return dst.releaseOwnership(this);
        }

        @Override public final Orphan<CABatch> project(Orphan<CABatch> dst, CABatch in) {
            if (dst == in)
                return projectInPlace(dst);
            return project0(setupDst(dst, false), in, in.cols).releaseOwnership(this);
        }

        private CABatch project0(CABatch dst, CABatch in, short ic) {
            short[] cols = columns;
            if (cols == null) throw new UnsupportedOperationException("not a projecting merger");
            boolean inPlace = dst == in;
            if (cols.length == 0)
                return mergeThin(dst, in);
            CABatch tail = inPlace ? dst : dst.tail();
            for (; in != null; in = in.next) {
                for (short ir = 0, iRows = in.rows, d, nr; ir < iRows; ir += nr) {
                    if (inPlace) {
                        d         = 0;
                        tail      = in;
                        tail.rows = nr = iRows; // do not change dirtyTerms
                        tail.cols = (short)cols.length;
                    } else {
                        d = (short)(tail.rows*tail.cols);
                        if (d+tail.cols > tail.termsCapacity) {
                            tail = createTail(dst);
                            d = 0;
                        }
                        tail.rows += nr = (short)min((tail.termsCapacity-d)/cols.length, iRows-ir);
                    }
                    for (short i=(short)(ir*ic), ie=(short)((ir+nr)*ic); i < ie; i+=ic) {
                        for (int c = 0, s; c < cols.length; c++, ++d) {
                            if ((s=cols[c]) < 0)
                                tail.setNull(d);
                            else
                                tail.copyTerm(d, in, i+s);
                        }
                    }
                }
            }
            assert dst != null;
            assert dst.validate();
            return dst;
        }

        @Override public final Orphan<CABatch> projectInPlace(Orphan<CABatch> orphan) {
            if (peekRows(orphan) == 0 || outColumns == 0)
                return projectInPlaceEmpty(orphan);
            short ic = peekColumns(orphan);
            var dst = setupDst(safeInPlaceProject ? orphan : null, safeInPlaceProject);
            var in = safeInPlaceProject ? dst : orphan.takeOwnership(this);
            try {
                return project0(dst, in, ic).releaseOwnership(this);
            } finally {
                if (in != dst) in.recycle(this);
            }
        }

        @Override public Orphan<CABatch> processInPlace(Orphan<CABatch> b) { return projectInPlace(b); }

        @Override public void onBatch(Orphan<CABatch> batch) {
            if (batch != null) {
                int rcvRows = peekTotalRows(batch);
                if (beforeOnBatch(batch))
                    afterOnBatch(projectInPlace(batch), rcvRows);
            }
        }
        @Override public void onBatchByCopy(CABatch batch) {
            if (batch != null) {
                int rcvRows = batch.totalRows();
                if (beforeOnBatch(batch))
                    afterOnBatch(project(fillingBatch(), batch), rcvRows);
            }
        }
    }

    public static abstract sealed class Filter extends BatchFilter<CABatch, Filter> {
        private final Filter beforeFilter;
        private final Merger projector;

        public Filter(BatchType<CABatch> batchType, Vars vars, @Nullable Orphan<Merger> projector,
                      Orphan<? extends RowFilter<CABatch, ?>> rowFilter,
                      @Nullable Orphan<? extends BatchFilter<CABatch, ?>> before) {
            super(batchType, vars, rowFilter, before);
            this.projector = Orphan.takeOwnership(projector, this);
            assert this.projector == null || this.projector.vars.equals(vars);
            this.beforeFilter = (Filter)this.before;
        }

        @Override protected void doRelease() {
            Owned.safeRecycle(projector, this);
            super.doRelease();
        }

        protected static final class Concrete extends Filter implements Orphan<Filter> {
            public Concrete(BatchType<CABatch> batchType, Vars vars,
                            @Nullable Orphan<Merger> projector,
                            Orphan<? extends RowFilter<CABatch, ?>> rowFilter,
                            @Nullable Orphan<? extends BatchFilter<CABatch, ?>> before) {
                super(batchType, vars, projector, rowFilter, before);
            }
            @Override public Filter takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public Orphan<CABatch> processInPlace(Orphan<CABatch> b) {
            return filterInPlace(b);
        }

        @Override public void onBatch(Orphan<CABatch> batch) {
            if (batch != null) {
                int rcvRows = peekTotalRows(batch);
                if (beforeOnBatch(batch))
                    afterOnBatch(filterInPlace(batch), rcvRows);
            }
        }

        @Override public Orphan<CABatch> filterInPlace(Orphan<CABatch> inOrphan) {
            if (beforeFilter != null)
                inOrphan = beforeFilter.filterInPlace(inOrphan);
            if (inOrphan == null)
                return null;
            var p = this.projector;
            if (p != null && rowFilter.targetsProjection()) {
                inOrphan = p.projectInPlace(inOrphan);
                p = null;
            }
            var filtered = inOrphan.takeOwnership(this);
            if (filtered.rows*outColumns == 0)
                return filterEmpty(filtered).releaseOwnership(this);
            if (!rowFilter.isNoOp()) {
                short cols   = filtered.cols, rows;
                var decision = DROP;
                CABatch next       = filtered;
                filtered     = null;
                while (next != null) {
                    var b    = next;
                    next     = Orphan.takeOwnership(next.detachHead(), this);
                    rows     = b.rows;
                    decision = DROP;
                    int d    = 0;
                    for (short r = 0, start; r < rows && decision != TERMINATE; r++) {
                        start = r;
                        while (r < rows && (decision = rowFilter.drop(b, r)) == KEEP) ++r;
                        if (r > start) {
                            int n = (r-start)*cols, srcPos = start*cols;
                            b.copy0(b, srcPos, d, n);
                            d += (short) n;
                        }
                    }
                    b.setRows(d/cols);
                    if      (d == 0)           Batch.safeRecycle(b, this);
                    else if (filtered == null) filtered = b;
                    else                       filtered.setTail(b.releaseOwnership(this));
                    if (decision == TERMINATE) {
                        cancelUpstream();
                        next = Batch.safeRecycle(next, this);
                    }
                }
                assert filtered == null || filtered.validate() : "corrupted";
                if (filtered == null && decision != TERMINATE)
                    filtered = batchType.create(outColumns).takeOwnership(this);
            }
            var resultOrphan = Owned.releaseOwnership(filtered, this);
            if (p != null && filtered != null && filtered.rows > 0)
                resultOrphan = p.projectInPlace(resultOrphan);
            return resultOrphan;
        }
    }
}
