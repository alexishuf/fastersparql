package com.github.alexishuf.fastersparql.sparql.results.serializer;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;

public class WsSerializer extends ResultsSerializer<WsSerializer> {
    public static final int DEF_BUFFER_HINT = 2048;
    private static final FinalSegmentRope PREFIX_CMD = FinalSegmentRope.asFinal("!prefix ");
    private static final class PrefixAssignerFac implements Supplier<WsPrefixAssigner> {
        @Override public WsPrefixAssigner get() {
            var pa = new WsPrefixAssigner(RopeArrayMap.create()).takeOwnership(RECYCLED);
            pa.reset();
            return pa;
        }
        @Override public String toString() {return "WsSerializer.PrefixAssignerFac";}
    }
    private static final class PrefixAssignerPrimerFac implements Supplier<WsPrefixAssigner> {
        @Override public WsPrefixAssigner get() {
            var ram = RopeArrayMap.create(new Rope[16]);
            var pa = new WsPrefixAssigner(ram).takeOwnership(RECYCLED);
            pa.reset();
            return pa;
        }
        @Override public String toString() {return "WsSerializer.PrefixAssignerPrimerFac";}
    }

    private static final Alloc<WsPrefixAssigner> PREFIX_ASSIGNER_ALLOC =
            new Alloc<>(WsPrefixAssigner.class, "WsSerializer.PREFIX_ASSIGNER_ALLOC",
                    Alloc.THREADS*64,
                    new PrefixAssignerFac(), 16 /* WsPrefixAssigner header */
                    + 4*2      /* PrefixAssigner fields */
                    + 4*2      /* WsPrefixAssigner fields */
                    + 16       /* RopeArrayMap header */
                    + 4*2      /* RopeArrayMap fields */
                    + 20+16*4  /* RopeArrayMap.data */
            );
    static {
        var fac = new PrefixAssignerPrimerFac();
        Primer.INSTANCE.sched(() -> PREFIX_ASSIGNER_ALLOC.prime(fac, 2, 0));
    }

    private final MutableRope rowsBuffer;
    private final WsPrefixAssigner prefixAssigner;

    public static class WsFactory implements Factory {
        @Override public Orphan<WsSerializer> create(Map<String, String> params) {
            return new WsSerializer.Concrete(DEF_BUFFER_HINT);
        }
        @Override public SparqlResultFormat name() { return SparqlResultFormat.WS; }
    }

    public static Orphan<WsSerializer> create(int bufferHint) {return new Concrete(bufferHint);}
    protected WsSerializer(int bufferHint) {
        super(SparqlResultFormat.WS.asMediaType());
        rowsBuffer     = new MutableRope(bufferHint);
        prefixAssigner = PREFIX_ASSIGNER_ALLOC.create();
        prefixAssigner.transferOwnership(RECYCLED, this);
    }
    private static final class Concrete extends WsSerializer implements Orphan<WsSerializer> {
        private Concrete(int bufferHint) {super(bufferHint);}
        @Override public WsSerializer takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override protected @Nullable WsSerializer internalMarkGarbage(Object currentOwner) {
        super.internalMarkGarbage(currentOwner);
        columns = null;
        vars    = Vars.EMPTY;
        ask     = false;
        empty   = true;
        prefixAssigner.recycle(this);
        rowsBuffer.close();
        return null;
    }

    @Override protected void onInit() {
        requireAlive();
        prefixAssigner.reset();
    }

    @Override public void serializeHeader(ByteSink<?, ?> dest) {
        requireAlive();
        for (int i = 0, n = subset.size(); i < n; i++) {
            if (i != 0) dest.append('\t');
            dest.append('?').append(subset.get(i));
        }
        dest.append('\n');
    }

    @Override
    public <B extends Batch<B>, T>
    void serialize(Orphan<B> orphan, ByteSink<?, T> sink, int hardMax,
                   NodeConsumer<B> nodeCons, ChunkConsumer<T> chunkCons) {
        if (orphan == null)
            return;
        B batch = orphan.takeOwnership(this);
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
            if (!chunkConsumer.isNoOp())
                deliver(sink, chunkConsumer, 1, 0, Integer.MAX_VALUE);
        }
        while (batch != null)
            batch = detachAndDeliverNode(batch, nodeConsumer);
    }

    private static final byte[] END = "!end\n".getBytes(UTF_8);
    @Override public void serializeTrailer(ByteSink<?, ?> dest) {
        requireAlive();
        dest.append(END);
    }

    private static final class WsPrefixAssigner extends PrefixAssigner
            implements Orphan<PrefixAssigner> {
        private @MonotonicNonNull ByteSink<?, ?> dest;

        private WsPrefixAssigner(Orphan<RopeArrayMap> map) {super(map);}

        @Override public @Nullable WsPrefixAssigner recycle(Object currentOwner) {
            internalMarkRecycled(currentOwner);
            if (PREFIX_ASSIGNER_ALLOC.offer(this) != null)
                internalMarkGarbage(RECYCLED);
            return null;
        }

        @Override public WsPrefixAssigner takeOwnership(Object newOwner) {
            takeOwnership0(newOwner);
            return this;
        }

        @Override public Rope nameFor(SegmentRope prefix) {
            Rope name = prefix2name.get(prefix);
            if (name == null) {
                name = RopeFactory.make(12).add('p').add(prefix2name.size()).take();
                prefix2name.put(FinalSegmentRope.asFinal(prefix), name);
                dest.ensureFreeCapacity(PREFIX_CMD.len+name.len()+ prefix.len()+3)
                      .append(PREFIX_CMD).append(name).append(':')
                      .append(prefix).append('>').append('\n');
            }
            return name;
        }
    }
}
