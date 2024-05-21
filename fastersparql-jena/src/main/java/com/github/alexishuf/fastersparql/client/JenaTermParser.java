package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.model.rope.RopeEncoder;
import com.github.alexishuf.fastersparql.org.apache.jena.atlas.io.AWriter;
import com.github.alexishuf.fastersparql.org.apache.jena.atlas.io.AWriterBase;
import com.github.alexishuf.fastersparql.org.apache.jena.graph.Node;
import com.github.alexishuf.fastersparql.org.apache.jena.riot.out.NodeFormatterNT;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.model.rope.RopeEncoder.MUTABLE_ROPE_APPENDER;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;

public abstract sealed class JenaTermParser extends AbstractOwned<JenaTermParser> {
    private record Fac(int initCap) implements Supplier<JenaTermParser> {
        @Override public JenaTermParser get() {
            return new Concrete(initCap).takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "JenaTermParser.Fac(" + initCap + ")";}
    }
    private static final Supplier<JenaTermParser> FAC = new Fac(64);
    private static final int BYTES = 16 + 3*4 + 4/*alignment*/ + 64 /*Concrete*/;
    private static final Alloc<JenaTermParser> ALLOC = new Alloc<>(JenaTermParser.class,
            "JenaTermParser.ALLOC", Alloc.THREADS*64, FAC, BYTES);
    static {
        Primer.INSTANCE.sched(() -> ALLOC.prime(new Fac(256), 1, 0));
    }
    private static final NodeFormatterNT fmt = new NodeFormatterNT();

    private final MutableRope tmp;
    private FinalSegmentRope shared;
    private final W writer;
    boolean isLit;
    public static Orphan<JenaTermParser> create() {
        return ALLOC.create().releaseOwnership(RECYCLED);
    }
    private JenaTermParser(int capacity) {
        shared  = FinalSegmentRope.EMPTY;
        tmp     = new MutableRope(capacity);
        writer = new W(tmp);
    }
    private static final class Concrete extends JenaTermParser implements Orphan<JenaTermParser> {
        @SuppressWarnings("unused") // add 64 bytes of padding against false sharing
        private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
        private Concrete(int capacity) {super(capacity);}
        @Override public JenaTermParser takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable JenaTermParser recycle(Object currentOwner) {
        internalMarkRecycled(currentOwner);
        if (ALLOC.offer(this) != null) { // full pool
            internalMarkGarbage(currentOwner);
            tmp.close();
        }
        return null;
    }

    private final static class W extends AWriterBase {
        private final MutableRope tmp;
        private W(MutableRope tmp) {this.tmp = tmp;}

        @Override public AWriter print(char ch) {
            tmp.ensureFreeCapacity(3);
            tmp.len = RopeEncoder.char2utf8(ch, tmp.u8(), tmp.len);
            return this;
        }
        @Override public AWriter print(char[] cbuf) {
            tmp.ensureFreeCapacity(cbuf.length);
            RopeEncoder.charSequence2utf8(cbuf, 0, cbuf.length, MUTABLE_ROPE_APPENDER, tmp);
            return this;
        }
        @Override public AWriter print(String string) {
            tmp.append(string);
            return this;
        }
        @Override public AWriter printf(String fmt, Object... arg) {
            tmp.append(String.format(fmt, arg));
            return this;
        }
        @Override public AWriter println(String object) {
            tmp.append(object).append('\n');
            return this;
        }
        @Override public AWriter println() {
            tmp.append('\n');
            return this;
        }
        @Override public AWriter flush() {return this;}
        @Override public void close() {}
    }

    public void parse(Node node) {
        isLit = node.isLiteral();
        tmp.clear();
        fmt.format(writer, node);
        shared = isLit ? SHARED_ROPES.internDatatypeOf(tmp, 0, tmp.len)
                       : SHARED_ROPES.internPrefixOf  (tmp, 0, tmp.len);
    }

    public <B extends Batch<B>> void putTerm(B dst, int col, @Nullable Node node) {
        if (node == null)
            return;
        parse(node);
        dst.putTerm(col, shared, tmp.segment, tmp.utf8, localOff(), localLen(), isLit);
    }

    public FinalSegmentRope      shared() { return shared; }
    public boolean         suffixShared() { return isLit; }
    public byte @NonNull[]    localUtf8() { return tmp.u8(); }
    public MemorySegment   localSegment() { return tmp.segment; }
    public int                 localOff() { return isLit ? 0 : shared.len; }
    public int                 localLen() { return tmp.len-shared.len; }

}
