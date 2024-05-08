package com.github.alexishuf.fastersparql.sparql.results.serializer;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.util.owned.Orphan;

import java.util.Map;

import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.TSV;

public abstract sealed class TsvSerializer extends ResultsSerializer<TsvSerializer> {

    public static class TsvFactory implements Factory {
        @Override public Orphan<TsvSerializer> create(Map<String, String>params) {
            if (!params.getOrDefault("charset", "utf-8").equalsIgnoreCase("utf-8"))
                throw new NoSerializerException("Cannot generate TSV in a format other than UTF-8");
            return new Concrete();
        }
        @Override public SparqlResultFormat name() { return TSV;}
    }

    public static Orphan<TsvSerializer> create() { return new Concrete(); }
    private TsvSerializer() {super(TSV.asMediaType());}

    private static final class Concrete extends TsvSerializer implements Orphan<TsvSerializer> {
        @Override public TsvSerializer takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public void serializeHeader(ByteSink<?, ?> dest) {
        for (int i = 0, n = subset.size(); i < n; ++i) {
            if (i > 0) dest.append('\t');
            dest.append('?').append(subset.get(i));
        }
        dest.append('\n');
    }

    @Override
    public <B extends Batch<B>, T>
    void serialize(Orphan<B> orphan, ByteSink<?, T> sink, int hardMax,
                   NodeConsumer<B> nodeConsumer, ChunkConsumer<T> chunkConsumer) {
        if (orphan == null)
            return;
        B batch = orphan.takeOwnership(this);
        if (batch.rows == 0) {
            detachAndDeliverNode(batch, nodeConsumer);
            return;
        }
        boolean chunk = !chunkConsumer.isNoOp();
        int r = 0;
        try {
            if (ask) {
                serializePositiveAsk(batch, sink, nodeConsumer, chunkConsumer);
                return;
            }
            int chunkRows = 0, lastLen = sink.len();
            int softMax = Math.min(hardMax, sink.freeCapacity());
            for (; batch != null; batch = detachAndDeliverNode(batch, nodeConsumer)) {
                r = 0;
                for (int rows = batch.rows; r < rows; ++r) {
                    serialize(batch, sink, r);
                    ++chunkRows;
                    if (chunk) {
                        int len = sink.len();
                        // send chunk "2 rows" from reaching softMax
                        if (len >= softMax-(len-lastLen <<1)) {
                            deliver(sink, chunkConsumer, chunkRows, lastLen, hardMax);
                            chunkRows = 0;
                            sink.touch();
                        }
                    }
                    lastLen = sink.len();
                }
            }
            if (chunk)
                deliver(sink, chunkConsumer, 1, sink.len(), hardMax);
        } catch (Throwable t) {
            handleNotSerialized(batch, r, nodeConsumer, t);
            throw t;
        }
    }

    @Override public void serialize(Batch<?> batch, ByteSink<?, ?> sink, int row) {
        for (int outCol = 0, inCol; outCol < columns.length; outCol++) {
            if (outCol != 0) sink.append('\t');
            if ((inCol = columns[outCol]) >= 0)
                batch.writeNT(sink, row, inCol);
        }
        sink.append('\n');
    }

    private <B extends Batch<B>, S extends ByteSink<S, T>, T>
    void serializePositiveAsk(B batch, ByteSink<S, T> sink, NodeConsumer<B> nodeConsumer,
                              ChunkConsumer<T> chunkConsumer) {
        sink.append('\n');
        if (!chunkConsumer.isNoOp())
            deliver(sink, chunkConsumer, 1, 0, Integer.MAX_VALUE);
        while (batch != null)
            batch = detachAndDeliverNode(batch, nodeConsumer);
    }

    @Override public void serializeTrailer(ByteSink<?, ?> dest) { }
}
