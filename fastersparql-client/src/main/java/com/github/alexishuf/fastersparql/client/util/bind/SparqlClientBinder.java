package com.github.alexishuf.fastersparql.client.util.bind;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.MonoPublisher;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.github.alexishuf.fastersparql.client.BindType.MINUS;

public final class SparqlClientBinder<R> implements Binder<R> {
    private final SparqlClient<R, ?> client;
    private final @Nullable SparqlConfiguration configuration;
    private final Merger<R> merger;
    private final BindType bindType;

    public SparqlClientBinder(SparqlClientBinder<R> other){
        this.client        = other.client;
        this.configuration = other.configuration;
        this.merger        = new Merger<>(other.merger);
        this.bindType      = other.bindType;
    }

    public SparqlClientBinder(RowOperations rowOps, List<String> bindingsVars,
                              SparqlClient<R, ?> client, CharSequence sparql,
                              @Nullable SparqlConfiguration configuration,
                              BindType bindType) {
        this.client = client;
        this.configuration = configuration;
        this.merger = new Merger<>(rowOps, bindingsVars, sparql, bindType);
        this.bindType = bindType;
    }

    @Override public FSPublisher<R> bind(R leftRow) {
        if (bindType == MINUS && merger.isProduct())
            return new MonoPublisher<>(leftRow);
        FSPublisher<R> pub = client.query(merger.bindSparql(leftRow), configuration).publisher();
        switch (bindType) {
            case JOIN:
                return new   ClientJoinProcessor<>(pub, leftRow, merger, false);
            case LEFT_JOIN:
                return new   ClientJoinProcessor<>(pub, leftRow, merger, true);
            case EXISTS:
                return new ClientExistsProcessor<>(pub, leftRow, false);
            case NOT_EXISTS:
            case MINUS:
                return new ClientExistsProcessor<>(pub, leftRow, true);
            default:
                throw new UnsupportedOperationException("Unexpected bindType="+bindType);
        }
    }

    @Override public List<String>      resultVars() { return merger.outVars(); }
    @Override public Binder<R> copyIfNotShareable() { return new SparqlClientBinder<>(this); }

    private static final class ClientJoinProcessor<T> extends AbstractProcessor<T, T> {
        private final T leftRow;
        private final boolean leftJoin;
        private boolean empty = true, completed = false;
        private final Merger<T> merger;

        public ClientJoinProcessor(FSPublisher<? extends T> source, T leftRow, Merger<T> merger,
                                   boolean leftJoin) {
            super(source);
            this.leftRow = leftRow;
            this.merger = merger;
            this.leftJoin = leftJoin;
        }

        @Override protected void handleOnNext(T rightRow) {
            empty = false;
            emit(merger.merge(leftRow, rightRow));
        }

        @Override protected void completeDownstream(@Nullable Throwable cause) {
            if (!completed && cause == null && empty && leftJoin)
                emit(merger.merge(leftRow, null));
            completed = true;
            super.completeDownstream(cause);
        }

        @Override public String toString() {
            String prefix = (leftJoin ? "Left" : "") + "ClientJoinProcessor";
            return prefix + merger.rowOps().toString(leftRow);
        }
    }

    private static final class ClientExistsProcessor<T> extends AbstractProcessor<T, T> {
        private final T leftRow;
        private final boolean negate;
        private boolean empty = true, completed = false;

        public ClientExistsProcessor(FSPublisher<? extends T> source, T leftRow, boolean negate) {
            super(source);
            this.leftRow = leftRow;
            this.negate = negate;
        }

        @Override protected void handleOnNext(T item) throws Exception {
            empty = false;
            cancelUpstream();
            completeDownstream(null);
        }

        @Override protected void completeDownstream(@Nullable Throwable cause) {
            if (!completed && cause == null && (!empty ^ negate))
                emit(leftRow);
            completed = true;
            super.completeDownstream(cause);
        }

        @Override public String toString() {
            return negate ? "ClientNotExistsProcessor" : "ClientExistsProcessor";
        }
    }
}
