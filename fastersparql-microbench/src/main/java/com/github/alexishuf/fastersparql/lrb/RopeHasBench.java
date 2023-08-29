package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.github.alexishuf.fastersparql.lrb.Workloads.fromName;
import static com.github.alexishuf.fastersparql.lrb.Workloads.uniformCols;

@SuppressWarnings("rawtypes")
@State(Scope.Thread)
@Threads(1)
@Fork(value = 1, warmups = 0, jvmArgsPrepend = {
        "--enable-preview",
        "--add-modules", "jdk.incubator.vector"})
//        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0"})
@Measurement(iterations = 10, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 4, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RopeHasBench {
    @Param({"23"})            private int seed;
    @Param({"ALL"})           private String sizeName;
    @Param({"NETTY", "BYTE"}) private RopeType ropeType;

    private SegmentRope leftContainer, rightContainer;
    private SegmentRope[] left, right;
    private RopeTypeHolder rt;

    @Setup(Level.Trial) public void setup() {
        rt = new RopeTypeHolder(ropeType);
        ByteSink sink = rt.byteSink().touch();
        int terms = 0;
        var batches = uniformCols(fromName(Batch.TERM, sizeName), Batch.TERM);
        for (TermBatch b : batches)
            terms += b.rows * b.cols;
        int[] offsets = new int[terms+1];
        terms = 0;
        for (TermBatch b : batches) {
            for (int r = 0, rows = b.rows, cols = b.cols; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    if (b.termType(r, c) != null) {
                        offsets[terms++] = sink.len();
                        b.writeNT(sink, r, c);
                    }
                }
            }
        }
        offsets[terms] = sink.len();

        leftContainer = rt.takeRope(sink);
        rightContainer = rt.takeRope(rt.byteSink().append(leftContainer));
        List<SegmentRope> ropes = new ArrayList<>(terms);
        for (int i = 0; i < terms; i++) {
            SegmentRope sub = leftContainer.sub(offsets[i], offsets[i + 1]);
            if (sub.len == 0) throw new AssertionError("empty string");
            ropes.add(sub);
        }
        Collections.shuffle(ropes, new Random(seed));
        left = ropes.toArray(SegmentRope[]::new);
        right = new SegmentRope[terms];
        for (int i = 0; i < left.length; i++)
            right[i] = rightContainer.sub((int) left[i].offset, (int) (left[i].offset+left[i].len));
    }

    @TearDown(Level.Trial) public void tearDown() {
        rt.close();
        left = right = null;
        leftContainer = rightContainer = null;
    }


    @Benchmark public int hasEquals() {
        int ok = 0;
        for (int i = 0; i < left.length; i++)
            ok += left[i].has(i&1, right[i]) ? 1 : 0;
        return ok;
    }

    @Benchmark public int hasRandomSameSize() {
        int ok = 0;
        for (int i = 0; i < left.length-1; i++) {
            SegmentRope l = left[i], r = right[i+1];
            if (l.has(i&1, r, i&1, Math.min(l.len, r.len)))
                ++ok;
        }
        return ok;
    }
}
