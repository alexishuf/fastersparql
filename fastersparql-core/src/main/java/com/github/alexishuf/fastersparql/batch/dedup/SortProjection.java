package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.util.concurrent.Bytes;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.BitSet;

public abstract sealed class SortProjection<P extends SortProjection<P>>
        extends AbstractOwned<P> {
    protected final int columns;
    private SortProjection(int columns) {this.columns = columns;}

    protected abstract int get(int col);
    protected abstract int set(int col, int encoded);

    public int columns() { return columns; }

    public static int encode(int column, boolean descending) {
        return descending ? ~column : column;
    }

    public static int           column(int   src) { return         src^(     src>>31) ; }
    public static byte          column(byte  src) { return (byte)( src^((int)src>>31)); }
    public static short         column(short src) { return (short)(src^((int)src>>31)); }

    public static int   diffMultiplier(int   src) { return 1|(src>>31) ; }

    public static Builder<OfByte> ofByte(int columns) {
        return new Builder<>(new OfByte.Concrete(columns));
    }
    public static abstract sealed class OfByte extends SortProjection<OfByte> {
        private  Bytes col2src;
        private OfByte(int columns) {
            super(columns);
            if (columns > Byte.MAX_VALUE)
                throw new IndexOutOfBoundsException("columns > 127");
            this.col2src = Bytes.atLeast(columns).takeOwnership(this);
        }
        private static final class Concrete  extends OfByte implements Orphan<OfByte> {
            private Concrete(int columns) {super(columns);}
            @Override public OfByte takeOwnership(Object o) {return takeOwnership0(o);}
        }
        @Override public @Nullable OfByte recycle(Object currentOwner) {
            internalMarkGarbage(currentOwner);
            col2src = col2src.recycle(this);
            return null;
        }

        @Override protected int get(int col) {
            if (col < 0 || col >= columns)
                throw new IndexOutOfBoundsException("col="+col+" not in [0, columns) range");
            return col2src.arr[col];
        }
        @Override protected int set(int col, int encoded) {
            if (col < 0 || col >= columns)
                throw new IndexOutOfBoundsException("col="+col+" not in [0, columns) range");
            if (column(encoded) >= columns)
                throw new IndexOutOfBoundsException("src="+ encoded +" not in [0, columns) range");
            return col2src.arr[col] = (byte) encoded;
        }

        public byte[] array() { return col2src.arr; }
    }


    public static final class Builder<P extends SortProjection<P>> {
        private @Nullable P p;
        private int nextCol;
        private Builder(Orphan<P> p) {this.p = p.takeOwnership(this);}
        private P current() {
            if (p == null)
                throw new IllegalStateException("Builder built");
            return p;
        }

        public @This Builder<P> add(int col, boolean descending) {
            P p = current();
            if (col < 0 || col > p.columns)
                throw new IndexOutOfBoundsException("col="+col+" not in [0, columns) range");
            for (int i = 0; i < nextCol; i++) {
                if (p.get(i) == col)
                    throw new IllegalArgumentException("col="+col+" already selected");
            }
            p.set(nextCol++, encode(col, descending));
            return this;
        }
        public @This Builder<P> asc(int column) {return add(column, false);}
        public @This Builder<P> des(int column) {return add(column,  true);}

        public Orphan<P> build() {
            var p = current();
            var used = new BitSet(p.columns);
            for (int i = 0; i < nextCol; i++)
                used.set(column(p.get(i)));
            for (int i = nextCol; i < p.columns ; i++) {
                int col = used.nextClearBit(0);
                used.set(col);
                p.set(i, col);
            }
            this.p = null;
            return p.releaseOwnership(this);
        }
    }
}
