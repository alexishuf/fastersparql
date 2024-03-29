package com.github.alexishuf.fastersparql.sparql.results.serializer;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import com.github.alexishuf.fastersparql.util.concurrent.GlobalAffinityShallowPool;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.lang.foreign.MemorySegment;
import java.util.Map;

import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;

public class WsSerializer extends ResultsSerializer {
    public static final int DEF_BUFFER_HINT = 2048;
    private static final ByteRope PREFIX_CMD = new ByteRope("!prefix ");
    private static final int POOL_COL = GlobalAffinityShallowPool.reserveColumn();

    private final ByteRope rowsBuffer;
    private final WsPrefixAssigner prefixAssigner;
    private boolean pooled;

    public static class WsFactory implements Factory {
        @Override public ResultsSerializer create(Map<String, String> params) {
            return WsSerializer.create(DEF_BUFFER_HINT);
        }
        @Override public SparqlResultFormat name() { return SparqlResultFormat.WS; }
    }

    public static WsSerializer create(int bufferHint) {
        var s = (WsSerializer) GlobalAffinityShallowPool.get(POOL_COL);
        if (s == null) return new WsSerializer(bufferHint);
        if (!s.pooled)
            throw new IllegalStateException("Pooled WsSerializer not marked as pooled");
        s.pooled = false;
        ByteRope buffer = s.rowsBuffer;
        if (buffer.freeCapacity() < bufferHint) {
            byte[] bigger = ArrayPool.BYTE.getAtLeast(bufferHint);
            if (bigger != null && bigger.length >= bufferHint) {
                buffer.recycleUtf8();
                buffer.wrapSegment(MemorySegment.ofArray(bigger), bigger, 0, 0);
            }
        }
        return s;
    }

    protected WsSerializer(int bufferHint) {
        super(SparqlResultFormat.WS.asMediaType());
        (prefixAssigner = new WsPrefixAssigner()).reset();
        rowsBuffer = new ByteRope(ArrayPool.bytesAtLeast(bufferHint), 0, 0);
    }

    public void recycle() {
        if (pooled)
            throw new IllegalStateException("recycle() on pooled WsSerializer");
        pooled = true;
        columns = null;
        vars = Vars.EMPTY;
        ask = false;
        empty = true;
        prefixAssigner.reset();
        if (GlobalAffinityShallowPool.offer(POOL_COL, this) != null)  // rejected
            rowsBuffer.recycleUtf8(); // at least try to recycle our byte[] buffer
    }

    @Override protected void onInit() {
        if (pooled) throw new IllegalStateException("use of recycle()d serializer");
        prefixAssigner.reset();
    }

    @Override public void serializeHeader(ByteSink<?, ?> dest) {
        if (pooled) throw new IllegalStateException("use of recycle()d serializer");
        for (int i = 0, n = subset.size(); i < n; i++) {
            if (i != 0) dest.append('\t');
            dest.append('?').append(subset.get(i));
        }
        dest.append('\n');
    }

    @Override
    public <B extends Batch<B>, S extends ByteSink<S, T>, T>
    void serialize(Batch<B> batch0, ByteSink<S, T> sink, int hardMax,
                   NodeConsumer<B> nodeCons, ChunkConsumer<T> chunkCons) {
        if (batch0 == null) return;

        @SuppressWarnings("unchecked") B batch = (B)batch0;
        int r = 0;
        try {
            prefixAssigner.dest = sink;
            if (rowsBuffer.len != 0) {
                throw new IllegalStateException("rowsBuffer not empty");
            } else if (ask) {
                serializeAsk(sink, batch, nodeCons, chunkCons);
                return;
            }

            boolean chunk = !chunkCons.isNoOp();
            int chunkRows = 0, lastLen = sink.len(), lastRowsBufferLen;
            int softMax = min(hardMax, min(sink.freeCapacity(), rowsBuffer.u8().length));
            for (; batch != null; batch = detachAndDeliverNode(batch, nodeCons)) {
                r = 0;
                for (int rows = batch.rows; r < rows; ++r) {
                    lastRowsBufferLen = rowsBuffer.len;
                    // write terms to rowsBuffer, !prefix command will be written to sink
                    for (int col : columns) {
                        batch.writeSparql(rowsBuffer, r, col, prefixAssigner);
                        rowsBuffer.append('\t');
                    }
                    if (columns.length == 0)
                        rowsBuffer.append((byte)'\n');
                    else
                        rowsBuffer.u8()[rowsBuffer.len - 1] = '\n'; // replace last '\t'
                    ++chunkRows;
                    int lastRowLen = rowsBuffer.len-lastRowsBufferLen;
                    // send when we are "2 rows" from reaching softMax
                    if (rowsBuffer.len + sink.len() >= softMax - lastRowLen<<1) {
                        sink.append(rowsBuffer); // flush serialization into sink
                        rowsBuffer.len = 0;
                        if (chunk) {
                            deliver(sink, chunkCons, chunkRows, lastLen, hardMax);
                            chunkRows = 0; // chunk delivered
                            sink.touch();
                        }
                    }
                    lastLen = sink.len();
                }
            }
            // deliver buffered rows
            if (rowsBuffer.len > 0 || sink.len() > 0) {
                sink.append(rowsBuffer);
                rowsBuffer.len = 0;
                if (chunk)
                    deliver(sink, chunkCons, 1, sink.len(), hardMax);
            }
        } catch (Throwable t) {
            handleNotSerialized(batch, r, nodeCons, t);
            throw t;
        }
    }

    @Override public void serialize(Batch<?> batch, ByteSink<?, ?> sink, int row) {
        if (rowsBuffer.len != 0)
            throw new IllegalStateException("rowsBuffer not empty");
        // write terms to rowsBuffer, !prefix command will be written to sink
        for (int col : columns) {
            batch.writeSparql(rowsBuffer, row, col, prefixAssigner);
            rowsBuffer.append('\t');
        }
        rowsBuffer.u8()[rowsBuffer.len - 1] = '\n'; // replace last '\t'
        sink.append(rowsBuffer);
        rowsBuffer.len = 0;
    }

    private <B extends Batch<B>, S extends ByteSink<S, T>, T>
    void serializeAsk(ByteSink<S, T> sink, B batch, NodeConsumer<B> nodeConsumer,
                              ChunkConsumer<T> chunkConsumer) {
        if (batch.rows > 0) {
            sink.append('\n');
            deliver(sink, chunkConsumer, 1, 0, Integer.MAX_VALUE);
        }
        while (batch != null)
            batch = detachAndDeliverNode(batch, nodeConsumer);
    }

    private static final byte[] END = "!end\n".getBytes(UTF_8);
    @Override public void serializeTrailer(ByteSink<?, ?> dest) {
        if (pooled) throw new IllegalStateException("Use of recycle()d serializer");
        dest.append(END);
    }

    private static final class WsPrefixAssigner extends PrefixAssigner {
        private @MonotonicNonNull ByteSink<?, ?> dest;

        public WsPrefixAssigner() {
            super(new RopeArrayMap());
        }

        @Override public Rope nameFor(SegmentRope prefix) {
            Rope name = prefix2name.get(prefix);
            if (name == null) {
                name = new ByteRope().append('p').append(prefix2name.size());
                prefix2name.put(prefix, name);
                dest.ensureFreeCapacity(PREFIX_CMD.len+name.len()+ prefix.len()+3)
                      .append(PREFIX_CMD).append(name).append(':')
                      .append(prefix).append('>').append('\n');
            }
            return name;
        }
    }
}
