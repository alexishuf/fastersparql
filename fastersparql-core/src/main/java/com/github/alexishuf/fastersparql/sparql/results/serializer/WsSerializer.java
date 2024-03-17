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
import com.github.alexishuf.fastersparql.util.concurrent.GlobalAffinityShallowPool;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.util.Map;

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

    public static WsSerializer create() { return create(DEF_BUFFER_HINT); }

    public static WsSerializer create(int bufferHint) {
        var s = (WsSerializer) GlobalAffinityShallowPool.get(POOL_COL);
        if (s == null) return new WsSerializer(bufferHint);
        if (!s.pooled)
            throw new IllegalStateException("Pooled WsSerializer not marked as pooled");
        s.pooled = false;
        return s;
    }

    protected WsSerializer(int bufferHint) {
        super(SparqlResultFormat.WS.asMediaType());
        (prefixAssigner = new WsPrefixAssigner()).reset();
        rowsBuffer = new ByteRope(bufferHint);
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

    @Override public void serialize(Batch<?> batch, int begin, int nRows, ByteSink<?, ?> dest) {
        prefixAssigner.dest = dest;
        for (int end = begin+nRows; begin < end; ++begin) {
            if (columns.length == 0) {
                dest.append('\n');
                continue;
            }
            // write terms to rowsBuffer, concurrently !prefix commands may be written to buffer
            for (int col : columns) {
                batch.writeSparql(rowsBuffer, begin, col, prefixAssigner);
                rowsBuffer.append('\t');
            }
            rowsBuffer.u8()[rowsBuffer.len-1] = '\n'; // replace last '\t' with line separator
            if ((begin&0xf) == 0xf) { // flush rowsBuffer to buffer once every 16 lines
                dest.append(rowsBuffer);
                rowsBuffer.clear();
            }
        }
        // always flush rowsBuffer on end
        dest.append(rowsBuffer);
        rowsBuffer.clear();
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
