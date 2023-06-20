package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;

public abstract class ResultsSender<S extends ByteSink<S, T>, T> implements AutoCloseable {
    protected final ResultsSerializer serializer;
    protected final S sink;

    public ResultsSender(ResultsSerializer serializer, S sink) {
        this.serializer = serializer;
        this.sink = sink;
    }

    @Override public void close() { sink.release(); }

    /** Sends the result of {@link ResultsSerializer#init(Vars, Vars, boolean, ByteSink)} */
    public abstract void sendInit(Vars vars, Vars subset, boolean isAsk);
    /** Sends the result of {@link ResultsSerializer#serialize(Batch, ByteSink)} */
    public abstract void sendSerialized(Batch<?> batch);
    /** Sends the result of {@link ResultsSerializer#serialize(Batch, int, int, ByteSink)} */
    public abstract void sendSerialized(Batch<?> batch, int from, int nRows);
    /** Sends the result of {@link ResultsSerializer#serializeTrailer(ByteSink)} */
    public abstract void sendTrailer();
    /**
     * Stops serialization and sends an error message representing the given exception.
     */
    public abstract void sendError(Throwable cause);
    /**
     * If the protocol supports, send a cancellation request, else close the connection or send
     * an error. In any case no more messages should be sent after this.
     */
    public abstract void sendCancel();
}