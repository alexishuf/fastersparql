package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.github.alexishuf.fastersparql.lrb.Workloads.fromName;
import static com.github.alexishuf.fastersparql.lrb.Workloads.uniformCols;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;

@SuppressWarnings({"unchecked", "rawtypes"})
@State(Scope.Thread)
@Threads(1)
@Fork(value = 1, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(iterations = 6, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 4, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RopeDictBench {

    @Param({"ALL"})           private String sizeName;
    @Param({"NETTY", "BYTE"}) private RopeType ropeType;

    private int[][] startsArray;
    private SegmentRope[] ropes;
    private RopeTypeHolder rt;

    @Setup(Level.Trial) public void setup() {
        rt = new RopeTypeHolder(ropeType);
        ByteSink sink = rt.byteSink();
        int terms = 0, i = 0;
        var batches = uniformCols(fromName(Batch.TERM, sizeName), Batch.TERM);
        for (TermBatch b : batches)
            terms += b.rows*b.cols;
        int[] starts = new int[terms+1];
        for (TermBatch b : batches) {
            for (int r = 0, rows = b.rows, cols = b.cols; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    starts[i++] = sink.len();
                    Term term = b.get(r, c);
                    if (term != null) sink.append(term);
                }
            }
        }
        starts[i] = sink.len();
        Rope rope = rt.takeRope(sink);
        startsArray = new int[64][];
        ropes = new SegmentRope[64];
        for (int rep = 0; rep < ropes.length; rep++) {
             ropes[rep] = rt.takeRope(rt.byteSink().append(rope));
             startsArray[rep] = Arrays.copyOf(starts, starts.length);
        }
        Workloads.cooldown(500);
    }

    @TearDown(Level.Trial) public void tearDown() {
        rt.close();
        Arrays.fill(ropes, null);
        ropes = null;
    }

    @Benchmark public long dictInternAll() {
        long acc = 0;
        for (int rep = 0; rep < startsArray.length; rep++) {
            int[] starts = startsArray[rep];
            var rope = ropes[rep];
            for (int i = 0, last = starts.length-1; i < last; i++) {
                int begin = starts[i], end = starts[i+1];
                acc ^= switch (begin < end ? rope.get(begin) : 0) {
                    case '<' -> RopeDict.internIri(rope, begin, end);
                    case '"' -> RopeDict.internLit(rope, begin, end);
                    default -> 0;
                };
            }
        }
        return acc;
    }

    @Benchmark public long internAll() {
        long acc = 0;
        for (int rep = 0; rep < startsArray.length; rep++) {
            int[] starts = startsArray[rep];
            var rope = ropes[rep];
            for (int i = 0, last = starts.length-1; i < last; i++) {
                int begin = starts[i], end = starts[i+1];
                acc ^= switch (begin < end ? rope.get(begin) : 0) {
                    case '<' -> SHARED_ROPES.internPrefixOf(rope, begin, end).len;
                    case '"' -> SHARED_ROPES.internDatatypeOf(rope, begin, end).len;
                    default -> 0;
                };
            }
        }
        return acc;
    }
}
