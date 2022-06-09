package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.util.bind.Binder;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.MonoPublisher;
import com.github.alexishuf.fastersparql.operators.impl.PlanMerger;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class PlanMergerBinder<R> implements Binder<R> {
    private static final AtomicInteger nextId = new AtomicInteger(0);
    private final PlanMerger<R> merger;
    private final BindType bindType;
    private final int id = nextId.getAndIncrement();

    private PlanMergerBinder(PlanMergerBinder<R> other) {
        this.merger = new PlanMerger<>(other.merger);
        this.bindType = other.bindType;
    }

    public PlanMergerBinder(BindType bindType, RowOperations rowOps, List<String> leftVars,
                            Plan<R> right) {
        this.merger = new PlanMerger<>(rowOps, leftVars, right, bindType);
        this.bindType = bindType;
    }

    @Override public String              toString() { return "PlanMergerBinder-"+id; }
    @Override public List<String>      resultVars() { return merger.outVars(); }
    @Override public Binder<R> copyIfNotShareable() { return new PlanMergerBinder<>(this); }

    @Override public FSPublisher<R> bind(R row) {
        Results<R> right = merger.bind(row).execute();
        if (bindType == BindType.MINUS && merger.isProduct())
            return new MonoPublisher<>(row);
        switch (bindType) {
            case JOIN:
            case LEFT_JOIN:
                return new JoinProcessor(right.publisher(), row);
            case EXISTS:
            case NOT_EXISTS:
            case MINUS:
                return new ExistsProcessor(right.publisher(), row);
            default:
                throw new UnsupportedOperationException("Unexpected bindType="+bindType);
        }
    }

    private final class JoinProcessor extends AbstractProcessor<R, R> {
        private final R leftRow;
        private boolean empty = true, completed = false;

        public JoinProcessor(FSPublisher<? extends R> source, R leftRow) {
            super(source);
            this.leftRow = leftRow;
        }

        @Override protected void handleOnNext(R rightRow) {
            empty = false;
            emit(merger.merge(leftRow, rightRow));
        }

        @Override protected void completeDownstream(@Nullable Throwable cause) {
            if (!completed && cause == null && empty && bindType == BindType.LEFT_JOIN)
                emit(merger.merge(leftRow, null));
            completed = true;
            super.completeDownstream(cause);
        }

        @Override public String toString() {
            String name = (bindType == BindType.LEFT_JOIN ? "Left" : "") + "JoinProcessor";
            return String.format("%s.%s{right=%s,leftRow=%s}", PlanMergerBinder.this, name,
                                 merger.right().name(), merger.rowOps().toString(leftRow));
        }
    }

    private final class ExistsProcessor extends AbstractProcessor<R, R> {
        private final R leftRow;
        private boolean empty = true, completed = false;

        public ExistsProcessor(FSPublisher<? extends R> source, R leftRow) {
            super(source);
            this.leftRow = leftRow;
        }

        @Override protected void handleOnNext(R item) {
            empty = false;
            cancelUpstream();
            completeDownstream(null);
        }

        @Override protected void completeDownstream(@Nullable Throwable cause) {
            if (!completed && cause == null && (!empty ^ (bindType != BindType.EXISTS)))
                emit(leftRow);
            completed = true;
            super.completeDownstream(cause);
        }

        @Override public String toString() {
            String name;
            switch (bindType) {
                case     EXISTS: name = "ExistsProcessor"; break;
                case NOT_EXISTS: name = "NotExistsProcessor"; break;
                case      MINUS: name = "MinusProcessor"; break;
                default:
                    throw new UnsupportedOperationException("Unexpected bindType="+bindType);
            }
            return String.format("%s.%s{right=%s,leftRow=%s}", PlanMergerBinder.this, name,
                                 merger.right().name(), merger.rowOps().toString(leftRow));
        }
    }
}
