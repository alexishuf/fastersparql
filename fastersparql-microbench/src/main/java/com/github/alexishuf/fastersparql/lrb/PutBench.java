package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
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
    private List<Batch> in;
    private List<Batch> out;
//    private List<CompressedBatch> inC;
//    private List<CompressedBatch> outC;

    @Param({"COMPRESSED"})
    public String type;
    @Param({"MID_AND_SMALL"}) //@Param({"SMALL", "MID", "BIG"})
    public String size;

    @Setup
    public void setup() {
        BatchType<?> bt = switch (type) {
            case "TERM" -> Batch.TERM;
            case "COMPRESSED" -> Batch.COMPRESSED;
            default -> throw new IllegalArgumentException();
        };
        assert bt != null;
        in = switch (size) {
            case "SMALL" -> Workloads.small(bt);
            case "MID" -> Workloads.mid(bt);
            case "MID_AND_SMALL" -> Workloads.midAndSmall(bt);
            case "BIG" -> Workloads.big(bt);
            default -> throw new IllegalArgumentException();
        };
        out = new ArrayList<>();
        for (Batch batch : in)
            out.add(bt.create(BIt.PREFERRED_MIN_BATCH, batch.cols, 0));
//        if (bt == Batch.COMPRESSED) {
//            inC  = (List<CompressedBatch>)(List<?>)in;
//            outC = (List<CompressedBatch>)(List<?>)out;
//        } else {
//            inC = null;
//            outC = null;
//        }
    }

    @Benchmark
    public List<Batch> put() {
        for (int i = 0, n = in.size(); i < n; i++) {
            Batch outBatch = out.get(i);
            Batch inBatch = in.get(i);
            outBatch.clear(inBatch.cols);
            outBatch.put(inBatch);
        }
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

    @Benchmark
    public List<Batch> putRow() {
        for (int i = 0, n = in.size(); i < n; i++) {
            Batch outBatch = out.get(i);
            Batch inBatch = in.get(i);
            outBatch.clear(inBatch.cols);
            for (int r = 0, rows = inBatch.rows; r < rows; r++)
                outBatch.putRow(inBatch, r);
        }
        return out;
    }
}
