package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@SuppressWarnings({"rawtypes", "unchecked"})
@State(Scope.Thread)
@Threads(8)
@Fork(value = 1, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(time = 250, iterations = 10, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 4, time = 250, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PutBench {
    private Batch in;
    private Batch out;
//    private List<CompressedBatch> inC;
//    private List<CompressedBatch> outC;

    @Param({"COMPRESSED"})
    public String typeName;
    @Param({"MID_AND_SMALL"}) //@Param({"SMALL", "MID", "BIG"})
    public String sizeName;

    @Setup
    public void setup() {
        BatchType<?> bt = Workloads.parseBatchType(typeName);
        assert bt != null;
        in  = Workloads.fromName(bt, sizeName).takeOwnership(this);
        out = bt.create(in.cols).takeOwnership(this);
    }

    @TearDown
    public void tearDown() {
        in = (Batch)in.recycle(this);
        out = (Batch)out.recycle(this);
    }

    @Benchmark public Batch put() {
        out.clear(in.cols);
        out.copy(in);
        return out;
    }

//    @Benchmark
//    public List<CompressedBatch> putScalar() {
//        if (inC == null || outC == null) return outC;
//        for (int i = 0, n = inC.size(); i < n; i++) {
//            var outBatch = outC.get(i);
//            var inBatch = inC.get(i);
//            outBatch.clear(inBatch.cols);
//            outBatch.pubScalarPut(inBatch);
//        }
//        return outC;
//    }

    @Benchmark public Batch putRow() {
        out.clear(in.cols);
        for (var n = in; n != null; n = n.next) {
            for (int r = 0, rows = n.rows; r < rows; r++)
                out.putRow(n, r);
        }
        return out;
    }
}
