package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermView;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.github.alexishuf.fastersparql.batch.type.UnitBatchType.UNIT;

public abstract sealed class UnitBatch extends Batch<UnitBatch> {
    static final Integer[] COL =
            IntStream.range(0, Short.MAX_VALUE).boxed().toArray(Integer[]::new);
    static final int BYTES = 16+16+8 + 16+6*4+16+16+16*(16+4*4);
    private static final Map<Integer, FinalTerm> EMPTY_MAP = Map.of();

    /**
     * This is extremely silly. However, the whole purpose of {@link UnitBatch} is violating all
     * architectural constraints and principles that govern batch implementations.
     *
     * <p>Most execution engines represent results as maps of variables into term objects. Since
     * batches in fastersparql do not have named columns, it would not be possible to have
     * such map without extensive changes to all code that uses batches. Therefore, the impact
     * of a  map from vars to terms is simulated by a map from integers to vars. To avoid
     * biasing conclusions, the kays for such map are fetched from a read-only array,
     * avoiding allocations.</p>
     */
    private Map<Integer, FinalTerm> map;

    public static Orphan<UnitBatch> create() { return new Concrete((short)0); }
    public static Orphan<UnitBatch> create(int cols) { return new Concrete((short)cols); }

    private UnitBatch(short cols) {
        super((short) 0, cols);
        this.map = cols == 0 ? EMPTY_MAP : new HashMap<>();
    }

    /* --- --- --- lifecycle --- --- --- */

    private static final class Concrete extends UnitBatch implements Orphan<UnitBatch> {
        public  Concrete(short cols) { super(cols); }
        @Override public UnitBatch takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable UnitBatch recycle(Object currentOwner) {
        return internalMarkGarbage(currentOwner);
    }

    /* --- --- --- batch-level accessors --- --- --- */

    @Override public BatchType<UnitBatch> type() {return UNIT;}

    /* --- --- --- batch-level accessors --- --- --- */

    @Override public int      termsCapacity() {return cols;}
    @Override public int totalBytesCapacity() {return cols*4;}
    @Override public int       rowsCapacity() {return 1;}

    @Override public boolean hasCapacity(int terms, int localBytes) {
        return terms <= cols;
    }

    @Override public Orphan<UnitBatch> dup() {
        Concrete copy = new Concrete(cols);
        if (cols == 0) {
            copy.addRowsToZeroColumns(totalRows());
        } else if (rows != 0) {
            UnitBatch in = this, dst = copy;
            while (in != null) {
                dst.rows = 1;
                dst.map.putAll(in.map);
                if ((in=in.next) != null)
                    copy.setTailAndReturnNull(dst=(new Concrete(cols).takeOwnership(dst)));
            }
        }
        return copy;
    }
    @Override public Orphan<UnitBatch> dup(int threadId) {return dup();}

    /* --- --- --- row-level accessors --- --- --- */

    @Override public Orphan<UnitBatch> dupRow(int row) {
        if (row != 0 || rows != 1)
            throw new IndexOutOfBoundsException();
        var copy = new Concrete(cols);
        copy.putRow(this, 0);
        return copy;
    }
    @Override public Orphan<UnitBatch> dupRow(int row, int threadId) {return dupRow(row);}

    /* --- --- --- term-level accessors --- --- --- */

    @Override public @Nullable FinalTerm get(@NonNegative int row, @NonNegative int col) {
        //noinspection ConstantValue
        if (row != 0 || rows != 1 || col < 0 || col >= cols)
            throw new IndexOutOfBoundsException();
        return map.getOrDefault(COL[col], null);
    }

    @Override public boolean getView(@NonNegative int row, @NonNegative int col, TermView dest) {
        //noinspection ConstantValue
        if (row != 0 || rows != 1 || col < 0 || col >= cols)
            throw new IndexOutOfBoundsException();
        var term = map.getOrDefault(COL[col], null);
        if (term == null)
            return false;
        dest.wrap(term);
        return true;
    }

    @Override public TermInfo.Type get(@NonNegative int row, @NonNegative int col, TermInfo info) {
        //noinspection ConstantValue
        if (row != 0 || rows != 1 || col < 0 || col >= cols)
            throw new IndexOutOfBoundsException();
        return info.setTerm(map.getOrDefault(COL[col], null));
    }

    @Override
    public boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest) {
        //noinspection ConstantValue
        if (row != 0 || rows != 1 || col < 0 || col >= cols)
            throw new IndexOutOfBoundsException();
        var term = map.getOrDefault(COL[col], null);
        if (term == null)
            return false;
        dest.wrapFirst(term.shared());
        dest.wrapSecond(term.local());
        if (term.sharedSuffixed())
            dest.flipSegments();
        return true;
    }

    /* --- --- --- batch-level mutators --- --- --- */

    @Override public void clear() {
        if (map != EMPTY_MAP)
            map.clear();
        rows = 0;
        tail = this;
        if (next != null)
            next = next.recycle(this);
    }

    @Override public @This UnitBatch clear(int newColumns) {
        if (newColumns > Short.MAX_VALUE)
            throw new IllegalArgumentException("Too many columns");
        rows = 0;
        cols = (short)newColumns;
        tail = this;
        if (next != null)
            next = next.recycle(this);
        if (map == EMPTY_MAP) {
            if (newColumns > 0)
                map = new HashMap<>();
        } else {
            map.clear();
        }
        return this;
    }

    @Override public void copy(UnitBatch other) {
        if (other.rows == 0)
            return; // no-op
        if (other.cols != cols)
            throw new IllegalArgumentException("other.cols != cols");
        if (cols == 0) {
            addRowsToZeroColumns(other.totalRows());
            return;
        } else if (rows == 0) {
            map.putAll(other.map);
            rows = 1;
            if ((other=other.next) == null)
                return;
        }
        setTail(other.dup());
    }

    @Override public void append(Orphan<UnitBatch> other) {
        short cols = this.cols;
        if (peekColumns(other) != cols)
            throw new IllegalArgumentException("other.cols !+ cols");
        UnitBatch newTail;
        if (rows == 0 && peekRows(other) != 0) {
            newTail = other.takeOwnership(tail);
            if (cols != 0)
                map.putAll(newTail.map);
            rows = 1;
            if ((newTail=newTail.dropHead(tail)) == null)
                return;
        } else {
            newTail = other.takeOwnership(tail);
        }
        if (newTail.rows == 0)
            Owned.safeRecycle(newTail, tail);
        else
            setTailAndReturnNull(newTail);
    }

    @Override public void putRow(UnitBatch other, int row) {
        short cols = this.cols;
        if (other.cols != cols)
            throw new IllegalArgumentException("other.cols != cols");
        if (row < 0 || row >= other.rows)
            throw new IndexOutOfBoundsException("row not in [0, other.rows)");
        if (cols == 0) {
            addRowsToZeroColumns(1);
        } else {
            var tail = this.tail;
            if (tail.rows != 0)
                tail = setTail(new Concrete(cols));
            tail.map.putAll(other.map);
            tail.rows++;
        }
    }

    @Override public void deFragmentMiddleNodes() {/* no fragmentation is possible */}

    /* --- --- --- row builder --- --- --- */

    @Override public void beginPut() {
        if (tail.rows != 0)
            setTail(create(cols));
    }

    @Override public void commitPut() {
        if (tail.rows != 0)
            throw new IllegalStateException("beginPut() not called");
        tail.rows = 1;
    }

    @Override public void abortPut() throws IllegalStateException {
        if (tail.rows != 0)
            throw new IllegalArgumentException("beginPut() not called");
        dropEmptyTail();
    }

    @Override public void putTerm(int col, Term t) {
        if (col < 0 || col >= cols)
            throw new IndexOutOfBoundsException();
        tail.map.put(COL[col], FinalTerm.asFinal(t));
    }



    @Override public void putNullTerm(int col) {putTerm(col, (FinalTerm)null);}


    /* --- --- --- operation objects --- --- --- */

    public static abstract sealed class Merger extends BatchMerger<UnitBatch, Merger> {
        private final short outColumns;
        public Merger(Vars outVars, short[] sources,
                      @Nullable Orphan<? extends BatchProcessor<UnitBatch, ?>> before) {
            super(UNIT, outVars, sources, before);
            if (outVars.size() > Short.MAX_VALUE)
                throw new IllegalArgumentException("too many columns");
            outColumns = (short)outVars.size();
        }

        public static Orphan<Merger>
        create(Vars outVars, short[] sources,
               @Nullable Orphan<? extends BatchProcessor<UnitBatch, ?>> before) {
            return new Concrete(outVars, sources, before);
        }

        private static final class Concrete extends Merger implements Orphan<Merger> {
            public Concrete(Vars outVars, short[] sources,
                            @Nullable Orphan<? extends BatchProcessor<UnitBatch, ?>> before) {
                super(outVars, sources, before);
            }
            @Override public Merger takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public void onBatch(Orphan<UnitBatch> orphan) {
            if (orphan != null) {
                int rcvRows = peekTotalRows(orphan);
                if (before != null)
                    orphan = before.processInPlace(orphan);
                if (beforeOnBatch(orphan))
                    afterOnBatch(projectInPlace(orphan), rcvRows);
            }
        }

        @Override public void onBatchByCopy(UnitBatch batch) {
            if (batch != null) {
                if (before != null) {
                    onBatch(batch.dup());
                } else {
                    int rcvRows = batch.totalRows();
                    if (beforeOnBatch(batch))
                        afterOnBatch(project(fillingBatch(), batch), rcvRows);
                }
            }
        }

        @Override public Orphan<UnitBatch> processInPlace(Orphan<UnitBatch> b) {
            return projectInPlace(b);
        }

        @Override public Orphan<UnitBatch> projectInPlace(Orphan<UnitBatch> orphan) {
            if (peekRows(orphan) == 0 || outColumns == 0)
                return projectInPlaceEmpty(orphan);
            var batch = orphan.takeOwnership(this);
            try {
                short[] columns = Objects.requireNonNull(this.columns);
                for (var node = batch; node != null; node = node.next) {
                    node.cols = outColumns;
                    if (node.rows == 0)
                        continue;
                    Map<Integer, FinalTerm> map = new HashMap<>(), in = node.map;
                    for (int c = 0, s; c < columns.length; c++)
                        map.put(COL[c], (s=columns[c]) < 0 ? null : in.getOrDefault(COL[s], null));
                    node.map = map;
                }
                return Objects.requireNonNull(batch).releaseOwnership(this);
            } catch (Throwable t) {
                Owned.safeRecycle(batch, this);
                throw t;
            }
        }

        @Override public Orphan<UnitBatch> project(Orphan<UnitBatch> dstOrphan, UnitBatch in) {
            if (dstOrphan == in)
                return projectInPlace(dstOrphan);
            var dst = (dstOrphan == null ? UnitBatch.create() :  dstOrphan).takeOwnership(this);
            try {
                if (dst.cols != outColumns) {
                    if (dst.rows == 0)
                        dst.clear(outColumns);
                    else
                        throw new IllegalArgumentException("dstOrphan.cols != outColumns");
                }
                short[] columns = Objects.requireNonNull(this.columns);
                for (var node = in; node != null; node = node.next) {
                    if (node.rows == 0)
                        continue;
                    dst.beginPut();
                    var nm = node.map;
                    for (int c = 0, s; c < columns.length; c++)
                        dst.putTerm(c, (s=columns[c]) < 0 ? null :  nm.getOrDefault(COL[s], null));
                    dst.commitPut();
                }
                return dst.releaseOwnership(this);
            } catch (Throwable t) {
                Owned.safeRecycle(dst, this);
                throw t;
            }
        }

        private Orphan<UnitBatch> mergeWithMissing(UnitBatch dst, UnitBatch left,
                                                   UnitBatch right) {
            int rows = right == null || right.rows == 0 ? 1 : right.totalRows();
            var lm = left.map;
            for (int r = 0; r < rows; r++) {
                dst.beginPut();
                for (short c = 0, src; c < sources.length; c++) {
                    src = sources[c];
                    dst.putTerm(c, src > 0 ? lm.getOrDefault(COL[src-1], null) : null);
                }
                dst.commitPut();
            }
            assert dst.validate();
            return dst.releaseOwnership(this);
        }

        @Override
        public Orphan<UnitBatch> merge(@Nullable Orphan<UnitBatch> dstOffer, UnitBatch left,
                                       int leftRow, @Nullable UnitBatch right) {
            if (peekRows(dstOffer) != 0 && peekColumns(dstOffer) != outColumns ) {
                Orphan.safeRecycle(dstOffer);
                throw new IllegalArgumentException("dstOffer.cols != outColumns");
            }
            if (leftRow < 0 || leftRow >= left.rows) {
                Orphan.safeRecycle(dstOffer);
                throw new IndexOutOfBoundsException("leftRow not in [0, left.rows)");
            }
            if (dstOffer == null)
                dstOffer = UnitBatch.create();
            var dst = dstOffer.takeOwnership(this);
            if (dst.cols != outColumns)
                dst.clear(outColumns);
            if (sources.length == 0)
                return mergeThin(dst, right).releaseOwnership(this);
            if (right == null || right.rows*right.cols == 0)
                return mergeWithMissing(dst, left, right);

            Map<Integer, FinalTerm> lm = left.map, rm, sm;
            for (var node = right; node != null; node = node.next) {
                if (node.rows == 0)
                    continue;
                rm = node.map;
                dst.beginPut();
                for (short c = 0, src; c < sources.length; c++) {
                    if      ((src=sources[c]) == 0)   continue;
                    else if ( src             >  0) { sm = lm; src--; }
                    else                            { sm = rm; src = (short)(-src-1); }
                    dst.tail.map.put(COL[c], sm.getOrDefault(COL[src], null));
                }
                dst.commitPut();
            }
            assert dst.validate();
            return dst.releaseOwnership(this);
        }
    }

    public abstract static sealed class Filter extends BatchFilter<UnitBatch, Filter> {
        private final @Nullable Merger projector;

        public Filter(Vars outVars, Orphan<Merger> projector,
                      Orphan<? extends RowFilter<UnitBatch, ?>> rowFilter,
                      @Nullable Orphan<? extends BatchProcessor<UnitBatch, ?>> before) {
            super(UNIT, outVars, rowFilter, before);
            this.projector = Orphan.takeOwnership(projector, this);
            assert this.projector == null || this.projector.vars.equals(outVars);
        }

        public static Orphan<Filter>
        create(Vars outVars, Orphan<Merger> projector,
               Orphan<? extends RowFilter<UnitBatch, ?>> rowFilter,
               @Nullable Orphan<? extends BatchProcessor<UnitBatch, ?>> before) {
            return new Concrete(outVars, projector, rowFilter, before);
        }

        private static final class Concrete extends Filter implements Orphan<Filter> {
            public Concrete(Vars outVars, Orphan<Merger> projector,
                            Orphan<? extends RowFilter<UnitBatch, ?>> rowFilter,
                            @Nullable Orphan<? extends BatchProcessor<UnitBatch, ?>> before) {
                super(outVars, projector, rowFilter, before);
            }
            @Override public Filter takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public Orphan<UnitBatch> filterInPlace(Orphan<UnitBatch> inOrphan) {
            if (inOrphan == null)
                return null;
            Merger p = projector;
            if (p != null && rowFilter.targetsProjection()) {
                inOrphan = p.processInPlace(inOrphan);
                p = null;
            }
            UnitBatch out = null, in = inOrphan.takeOwnership(this);
            if (in.rows*outColumns == 0)
                return filterEmpty(in).releaseOwnership(this);
            RowFilter.Decision decision = RowFilter.Decision.DROP;
            while (in != null) {
                if (in.rows == 0) {
                    in = in.dropHead(this);
                } else {
                    switch (decision=rowFilter.drop(in, 0)) {
                        case TERMINATE -> in = Owned.safeRecycle(in, this);
                        case DROP      -> in = in.dropHead(this);
                        case KEEP      -> {
                            var next = Orphan.takeOwnership(in.detachHead(), this);
                            if (out == null)
                                out = in;
                            else
                                out.setTailAndReturnNull(in.transferOwnership(this, out.tail));
                            in = next;
                        }
                        case null, default -> throw new IllegalArgumentException();
                    }
                }
            }
            if (decision != RowFilter.Decision.TERMINATE && out == null)
                return UnitBatch.create(outColumns);
            var outOrphan = Owned.releaseOwnership(out, this);
            if (p != null && outOrphan != null && out.rows > 0)
                outOrphan = p.projectInPlace(outOrphan);
            return outOrphan;
        }

        @Override public Orphan<UnitBatch> processInPlace(Orphan<UnitBatch> b) {
            return filterInPlace(b);
        }

        @Override public void onBatch(Orphan<UnitBatch> batch) {
            if (batch != null) {
                int rcvRows = peekTotalRows(batch);
                if (before != null)
                    batch = before.processInPlace(batch);
                if (beforeOnBatch(batch))
                    afterOnBatch(filterInPlace(batch), rcvRows);
            }
        }
    }
}
