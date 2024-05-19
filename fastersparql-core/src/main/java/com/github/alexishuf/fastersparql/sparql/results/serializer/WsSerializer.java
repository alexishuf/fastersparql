package com.github.alexishuf.fastersparql.sparql.results.serializer;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.PrefixMap;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.DT_MID;
import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.asFinal;
import static com.github.alexishuf.fastersparql.model.rope.Rope.contains;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.*;
import static com.github.alexishuf.fastersparql.sparql.expr.SparqlSkip.PN_LOCAL_LAST;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;

public class WsSerializer extends ResultsSerializer<WsSerializer> {
    private static final FinalSegmentRope PREFIX_CMD = asFinal("!prefix ");
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

    private final WsPrefixAssigner prefixAssigner;
    private FinalSegmentRope[] names = ArrayAlloc.EMPTY_F_SEG_ROPE;
    private short[] datatypeTails = ArrayAlloc.EMPTY_SHORT;
    private final SegmentRopeView local = new SegmentRopeView();

    public static class WsFactory implements Factory {
        @Override public Orphan<WsSerializer> create(Map<String, String> params) {
            return new WsSerializer.Concrete();
        }
        @Override public SparqlResultFormat name() { return SparqlResultFormat.WS; }
    }

    public static Orphan<WsSerializer> create() {return new Concrete();}
    protected WsSerializer() {
        super(SparqlResultFormat.WS.asMediaType());
        prefixAssigner = PREFIX_ASSIGNER_ALLOC.create();
        prefixAssigner.transferOwnership(RECYCLED, this);
    }
    private static final class Concrete extends WsSerializer implements Orphan<WsSerializer> {
        private Concrete() {super();}
        @Override public WsSerializer takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override protected @Nullable WsSerializer internalMarkGarbage(Object currentOwner) {
        super.internalMarkGarbage(currentOwner);
        columns       = null;
        vars          = Vars.EMPTY;
        ask           = false;
        empty         = true;
        names         = ArrayAlloc.recycleSegmentRopesAndGetEmpty(names);
        datatypeTails = ArrayAlloc.recycleShortsAndGetEmpty(datatypeTails);
        prefixAssigner.recycle(this);
        return null;
    }

    @Override protected void onInit() {
        requireAlive();
        prefixAssigner.reset();
        names         = ArrayAlloc.finalSegmentRopesAtLeast(columns.length, names);
        datatypeTails = ArrayAlloc.shortsAtLeast(columns.length, datatypeTails);
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
            if (ask) {
                serializeAsk(sink, batch, nodeCons, chunkCons);
                return;
            }

            int chunkRows = 0, lastLen = sink.len(), lastRowBegin;
            boolean chunk     = !chunkCons.isNoOp();
            int softMax       = min(hardMax, sink.freeCapacity());
            var names         = this.names;
            var datatypeTails = this.datatypeTails;
            var columns       = this.columns;
            for (; batch != null; batch = detachAndDeliverNode(batch, nodeCons)) {
                r = 0;
                for (int rows = batch.rows; r < rows; ++r) {
                    lastRowBegin = sink.len();
                    if (columns.length == 0) {
                        sink.append((byte)'\n');
                    } else {
                        for (int i = 0; i < columns.length; i++) {
                            assignNames(batch, r, columns, i, names, datatypeTails);
                        }
                        for (int i = 0, last = columns.length - 1; i < columns.length; i++) {
                            writeSparql(sink, batch, r, columns, i, names, datatypeTails);
                            sink.append(i == last ? (byte)'\n' : (byte)'\t');
                        }
                    }
                    ++chunkRows;
                    int lastRowLen = sink.len()-lastRowBegin;
                    // send when we are "2 rows" from reaching softMax
                    if (chunk && sink.len() >= softMax - lastRowLen<<1) {
                        deliver(sink, chunkCons, chunkRows, lastLen, hardMax);
                        chunkRows = 0; // chunk delivered
                        sink.touch();
                    }
                    lastLen = sink.len();
                }
            }
            // deliver buffered rows
            if (chunk && sink.len() > 0)
                deliver(sink, chunkCons, 1, sink.len(), hardMax);
        } catch (Throwable t) {
            handleNotSerialized(batch, r, nodeCons, t);
            throw t;
        }
    }

    private boolean hasExp(Batch<?> batch, int r, int c) {
        if (!batch.localView(r, c, local))
            return false;
        final int len = local.len;
        return local.skipUntilLast(1, len, (byte)'e', (byte)'E') < len;
    }



    private void assignNames(Batch<?> batch, int r, int[] columns, int ci, FinalSegmentRope[] names,
                             short[] datatypeTails) {
        var sh = batch.shared(r, columns[ci]);
        FinalSegmentRope name;
        short tail = -1;
        if (sh.len > 15/*"^^<http://a#t>*/ && sh.get(0) == '"') {
            if (sh == DT_DOUBLE && hasExp(batch, r, columns[ci])) {
                name = NAKED;
            } else if (sh == DT_integer || sh == DT_decimal || sh == DT_BOOLEAN) {
                name = NAKED;
            } else {
                var prefix = SHARED_ROPES.internPrefixOf(sh, 3/*"^^<*/, sh.len);
                tail = (short)(sh.len-1-(3+prefix.len));
                name = (FinalSegmentRope)prefixAssigner.nameFor(prefix);
            }
        } else if (sh == P_RDF) {
            name = PrefixMap.RDF_NAME;
        } else if (sh.len > 0) {
            name = sh.get(0) == '"' ? NT_LIT : (FinalSegmentRope)prefixAssigner.nameFor(sh);
        } else {
            name = NT_LOCAL;
        }
        names[ci]         = name;
        datatypeTails[ci] = tail;
    }

    private static final FinalSegmentRope NAKED         = asFinal(new byte[]{'~'});
    private static final FinalSegmentRope NT_LIT        = asFinal(new byte[]{'?'});
    private static final FinalSegmentRope NT_LOCAL      = asFinal(new byte[]{'@'});

    private void writeSparql(ByteSink<?,?> dst, Batch<?> batch, int r,
                             int[] columns, int ci,
                             FinalSegmentRope[] names, short[] datatypeTails) {
        int column = columns[ci];
        if (!batch.localView(r, column, local))
            local.wrapEmpty();
        var name = names[ci];
        if (name == NAKED || name == NT_LOCAL || name == NT_LIT) {
            dst.append(local, name == NAKED ? 1 : 0, local.len);
            if (name == NT_LIT)
                dst.append(batch.shared(r, column));
        } else {
            short tail = datatypeTails[ci];
            if (tail >= 0) { // datatype
                dst.append(local, 0, local.len);
                dst.append(DT_MID).append(name).append(':');
                var sh = batch.shared(r, column);
                dst.append(sh, sh.len-1-tail, sh.len-1);
            } else if (name == PrefixMap.RDF_NAME && local.equals(TYPE_LOCAL)) { // a
                dst.append('a');
            } else { // IRI
                dst.append(name).append(':');
                byte bad = local.len < 2 ? (byte)'a' : local.get(local.len-2);
                boolean needsEscape = !contains(PN_LOCAL_LAST, bad)
                                   && !local.isEscaped(local.len-2);
                dst.append(local, 0, local.len-(needsEscape ? 2 : 1));
                if (needsEscape)
                    dst.append('\\').append(bad);
            }
        }
    }
    private static final SegmentRope TYPE_LOCAL = Term.RDF_TYPE.local();

    @Override public void serialize(Batch<?> batch, ByteSink<?, ?> sink, int row) {
        var names         = this.names;
        var datatypeTails = this.datatypeTails;
        int[] columns     = this.columns;
        for (int i = 0; i < columns.length; i++)
            assignNames(batch, row, columns, i, names, datatypeTails);
        for (int i = 0, last = columns.length - 1; i < columns.length; i++) {
            writeSparql(sink, batch, row, columns, i, names, datatypeTails);
            sink.append(i == last ? (byte)'\n' : (byte)'\t');
        }
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
                prefix2name.put(asFinal(prefix), name);
                dest.ensureFreeCapacity(PREFIX_CMD.len+name.len()+ prefix.len()+3)
                      .append(PREFIX_CMD).append(name).append(':')
                      .append(prefix).append('>').append('\n');
            }
            return name;
        }
    }
}
