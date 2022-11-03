package com.github.alexishuf.fastersparql.client.util.bind;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.operators.FilteringTransformBIt;
import com.github.alexishuf.fastersparql.batch.operators.FlatMapBIt;
import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.sparql.binding.RowBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.NoSuchElementException;

import static com.github.alexishuf.fastersparql.client.util.Merger.forMerge;

public abstract class BindingBIt<R, I> extends FlatMapBIt<R, R> {
    protected final Merger<R, I> merger;
    protected final BindType bindType;
    protected final RowBinding<R, I> tempBinding;

    public BindingBIt(BIt<R> left, BindType type, RowType<R, I> rowType,
                      Vars leftPublicVars, Vars rightPublicVars,
                      @Nullable Vars projection) {
        super(left.elementClass(), left, type.resultVars(leftPublicVars, rightPublicVars));
        Vars rFree = rightPublicVars.minus(leftPublicVars);
        this.merger = forMerge(rowType, leftPublicVars, rFree, type, projection);
        this.bindType = type;
        this.tempBinding = new RowBinding<>(rowType, leftPublicVars);
    }

    protected abstract BIt<R> bind(R left);

    @Override protected final BIt<R> map(R left) {
        BIt<R> right = bind(left);
        return switch (bindType) {
            case JOIN -> new Join<>(merger, left, right);
            case LEFT_JOIN -> new LeftJoin<>(merger, left, right);
            case MINUS,EXISTS,NOT_EXISTS -> new Exists<>(bindType, merger, left, right);
        };
    }

    private static final class Join<R, I> extends FilteringTransformBIt<R, R> {
        private final Merger<R, I> merger;
        private final R left;

        public Join(Merger<R, I> merger, R left, BIt<R> right) {
            super(right, merger.rowType().rowClass, merger.outVars);
            this.merger = merger;
            this.left = left;
        }

        @Override protected Batch<R> process(Batch<R> b) {
            R[] a = b.array;
            for (int i = 0, n = b.size; i < n; i++)
                a[i] = merger.merge(left, a[i]);
            return b;
        }

        @Override protected String toStringNoArgs() {
            return "Join["+merger.rowType.toString(left)+"]";
        }
    }

    private static final class LeftJoin<R, I> extends FilteringTransformBIt<R, R> {
        private final Merger<R, I> merger;
        private final R left;
        private boolean empty = true, introduced;

        public LeftJoin(Merger<R, I> merger, R left, BIt<R> right) {
            super(right, merger.rowType.rowClass, merger.outVars);
            this.merger = merger;
            this.left = left;
        }

        @Override protected Batch<R> process(Batch<R> b) {
            if (b.size != 0) {
                empty = false;
                R[] a = b.array;
                for (int i = 0, n = b.size; i < n; i++)
                    a[i] = merger.merge(left, a[i]);
            }
            return b;
        }

        @Override protected Batch<R> terminal() {
            if (empty && !introduced) {
                introduced = true;
                var b = recycleOrAlloc(1);
                b.array[0] = merger.merge(left, null);
                b.size = 1;
                return b;
            } else {
                return Batch.terminal();
            }
        }

        @Override protected String toStringNoArgs() {
            return "LeftJoin["+merger.rowType.toString(left)+"]";
        }
    }

    private static final class Exists<R, I> extends DelegatedControlBIt<R, R> {
        private final Merger<R, I> merger;
        private final BindType bindType;
        private final R left;
        private boolean exhausted;

        public Exists(BindType bindType, Merger<R, I> merger, R left, BIt<R> right) {
            super(right.tempEager(), merger.rowType.rowClass, merger.outVars);
            this.left = left;
            this.merger = merger;
            this.bindType = bindType;
        }

        @Override public Batch<R> nextBatch() {
            boolean negate = bindType != BindType.EXISTS;
            boolean empty = exhausted || ( negate ^ (delegate.nextBatch().size == 0) );
            exhausted = true;
            Batch<R> b = empty ? Batch.terminal() : new Batch<>(delegate.elementClass(), 10);
            if (!empty) {
                b.array[0] = left;
                b.size = 1;
            }
            return b;
        }

        @Override public boolean hasNext() {
            boolean negate = bindType != BindType.EXISTS;
            return !exhausted && (negate ^ delegate.tempEager().hasNext());
        }

        @Override public R next() {
            if (!hasNext()) throw new NoSuchElementException();
            exhausted = true;
            return left;
        }

        @Override public boolean recycle(Batch<R> batch) { return true; }

        @Override public String toString() {
            String name = switch (bindType) {
                case EXISTS     -> "Exists";
                case NOT_EXISTS -> "NotExists";
                case MINUS      -> "Minus";
                default         -> bindType.name();
            };
            return name+'['+merger.rowType.toString(left)+"]("+delegate+')';
        }
    }
}
