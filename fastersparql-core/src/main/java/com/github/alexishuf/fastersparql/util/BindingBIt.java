package com.github.alexishuf.fastersparql.util;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.operators.FilteringTransformBIt;
import com.github.alexishuf.fastersparql.batch.operators.FlatMapBIt;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.binding.RowBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.NoSuchElementException;

public abstract class BindingBIt<R> extends FlatMapBIt<R, R> {
    protected final RowType<R>.Merger merger;
    protected final BindType bindType;
    protected final RowBinding<R> tempBinding;
    protected final Metrics.@Nullable JoinMetrics metrics;

    public BindingBIt(BIt<R> left, BindType type, Vars leftPublicVars, Vars rightPublicVars,
                      @Nullable Vars projection,
                      Metrics. @Nullable JoinMetrics metrics) {
        super(left.rowType(), left,
              projection != null ? projection : type.resultVars(leftPublicVars, rightPublicVars));
        Vars rFree = rightPublicVars.minus(leftPublicVars);
        this.merger      = rowType.merger(vars(), leftPublicVars, rFree);
        this.bindType    = type;
        this.tempBinding = new RowBinding<>(rowType, leftPublicVars);
        this.metrics     = metrics;
    }

    protected abstract BIt<R> bind(R left);

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (metrics != null) metrics.completeAndDeliver(cause, this);
    }

    @Override public Batch<R> nextBatch() {
        Batch<R> b = super.nextBatch();
        if (metrics != null)  metrics.rightRowsReceived(b.size);
        return b;
    }

    @Override public R next() {
        R next = super.next();
        if (metrics != null) metrics.rightRowsReceived(1);
        return next;
    }

    @Override protected final BIt<R> map(R left) {
        if (metrics != null) metrics.beginBinding();
        BIt<R> right = bind(left);
        return switch (bindType) {
            case JOIN -> new Join<>(merger, left, right);
            case LEFT_JOIN -> new LeftJoin<>(merger, left, right);
            case MINUS,EXISTS,NOT_EXISTS -> new Exists<>(bindType, merger, left, right);
        };
    }

    protected abstract Object right();

    @Override protected String toStringNoArgs() {
        return super.toStringNoArgs()+'['+bindType+']';
    }

    @Override public String toString() {
        return toStringWithOperands(List.of(original, right()));
    }

    private static final class Join<R> extends FilteringTransformBIt<R, R> {
        private final RowType<R>.Merger merger;
        private final R left;

        public Join(RowType<R>.Merger merger, R left, BIt<R> right) {
            super(right, merger.rowType(), merger.outVars);
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
            return "Join["+merger.rowType().toString(left)+"]";
        }
    }

    private static final class LeftJoin<R> extends FilteringTransformBIt<R, R> {
        private final RowType<R>.Merger merger;
        private final R left;
        private boolean empty = true, introduced;

        public LeftJoin(RowType<R>.Merger merger, R left, BIt<R> right) {
            super(right, merger.rowType(), merger.outVars);
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
            return "LeftJoin["+merger.rowType().toString(left)+"]";
        }
    }

    private static final class Exists<R> extends DelegatedControlBIt<R, R> {
        private final RowType<R>.Merger merger;
        private final BindType bindType;
        private final R left;
        private boolean exhausted;

        public Exists(BindType bindType, RowType<R>.Merger merger, R left, BIt<R> right) {
            super(right.tempEager(), merger.rowType(), merger.outVars);
            this.left = left;
            this.merger = merger;
            this.bindType = bindType;
        }

        @Override public Batch<R> nextBatch() {
            boolean negate = bindType != BindType.EXISTS;
            boolean empty = exhausted || ( negate ^ (delegate.nextBatch().size == 0) );
            exhausted = true;
            Batch<R> b = empty ? Batch.terminal() : new Batch<>(delegate.rowType().rowClass, 10);
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

        @Override public String toString() {
            String name = switch (bindType) {
                case EXISTS     -> "Exists";
                case NOT_EXISTS -> "NotExists";
                case MINUS      -> "Minus";
                default         -> bindType.name();
            };
            return name+'['+merger.rowType().toString(left)+"]("+delegate+')';
        }
    }
}
