package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.results.WsBindingSeq;
import com.github.alexishuf.fastersparql.util.IOUtils;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;

@State(Scope.Thread)
@Threads(1)
@Fork(value = 3, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(iterations = 10, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class WsBindingSeqBench {
    private static final int N_BATCHES = 1<<8;
    private static final int BATCHES_MASK = N_BATCHES-1;
    private static final int N_ROWS = (1<<12) + (1<<9);

    @Param({"NETTY"}) private RopeType ropeType;

    private final List<CompressedBatch> batches = new ArrayList<>();
    private final List<SegmentRope> containers = new ArrayList<>();
    private final int[] begins = new int[N_ROWS], ends = new int[N_ROWS];
    private int nextInput;
    private RopeTypeHolder ropeTypeHolder;

    @Setup(Level.Iteration) public void setup() {
        ropeTypeHolder = new RopeTypeHolder(ropeType);
        var wbs = new WsBindingSeq();
        var filler = FinalSegmentRope.asFinal("\"123456789012345678901\"");
        var seedBatch = COMPRESSED.create(2).takeOwnership(this);
        try (MutableRope seedRope = new MutableRope(N_ROWS * 32)) {
            TwoSegmentRope tsr = new TwoSegmentRope();
            for (int i = 0; i < N_ROWS; i++) {
                seedBatch.beginPut();
                wbs.write(i, seedBatch, 0);
                seedBatch.putTerm(1, null, filler.utf8, 0, filler.len, true);
                seedBatch.commitPut();

                CompressedBatch tail = seedBatch.tail();
                int last = tail.rows - 1;
                if (!seedBatch.getRopeView(last, 0, tsr))
                    throw new AssertionError("put not visible");
                seedRope.append(tsr).append('\t');
                if (!seedBatch.getRopeView(last, 1, tsr))
                    throw new AssertionError("put not visible");
                seedRope.append(tsr).append('\n');
            }
            batches.clear();
            for (int i = 0; i < N_BATCHES; i++)
                batches.add(seedBatch.dup().takeOwnership(this));
            seedBatch.recycle(this);
            for (int i = 0, begin = 0, len = seedRope.len; begin < len; ++i) {
                begins[i] = begin;
                ends[i] = seedRope.skipUntil(begin, len, (byte)'\t');
                if (seedRope.get(begin) != '"') throw new AssertionError("bad begin");
                if (seedRope.get(ends[i] - 1) != '"') throw new AssertionError("bad end");
                begin = seedRope.skipUntil(begin, len, (byte)'\n') + 1;
            }
            containers.clear();
            ByteSink<?, ?> sink = ropeTypeHolder.byteSink();
            for (int i = 0; i < N_BATCHES; i++)
                containers.add(ropeTypeHolder.takeRope(sink.touch().append(seedRope)));
        }
    }

    @TearDown(Level.Iteration) public void iterationTearDown() {
        ropeTypeHolder.close();
        IOUtils.fsync(1_000);
        for (CompressedBatch b : batches)
            b.recycle(this);
        batches.clear();
        Async.uninterruptibleSleep(50);
    }

    @Benchmark public long parseFromBatch() {
        var b = batches.get(nextInput);
        nextInput = (nextInput +1) & BATCHES_MASK;
        try (var tmp = PooledSegmentRopeView.ofEmpty()) {
            long acc = 0;
            for (var n = b; n != null; n = n.next) {
                for (int r = 0, rows = n.rows; r < rows; r++) {
                    if (n.localView(r, 0, tmp))
                        acc += WsBindingSeq.parse(tmp, 0, tmp.len);
                }
            }
            return acc;
        }
    }

    @Benchmark public long parseFromSegmentRope() {
        SegmentRope container = containers.get(nextInput);
        nextInput = (nextInput+1) & BATCHES_MASK;
        long acc = 0;
        for (int r = 0; r < N_ROWS; r++)
            acc += WsBindingSeq.parse(container, begins[r], ends[r]);
        return acc;
    }

}
