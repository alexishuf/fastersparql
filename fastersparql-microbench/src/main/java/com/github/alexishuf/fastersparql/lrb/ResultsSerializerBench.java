package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"unchecked", "rawtypes"})
@State(Scope.Thread)
@Threads(1)
@Fork(value = 1, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 4, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ResultsSerializerBench {
    @Param({"COMPRESSED", "TERM"}) private String typeName;
    @Param("ALL") private String sizeName;
    @Param({"NETTY", "BYTE"}) private RopeType ropeType;
    @Param({"TSV", "WS", "JSON"}) private SparqlResultFormat format;
    @Param({"64"}) private int nLists;

    private RopeTypeHolder ropeTypeHolder;
    private Vars vars;
    private final List<Batch> batchLists = new ArrayList<>();
    private int nextBatchList = 0;

    @Setup(Level.Trial) public void setup() {
        BatchType bt = Workloads.parseBatchType(typeName);
        ropeTypeHolder = new RopeTypeHolder(ropeType);
        batchLists.add(Workloads.fromName(bt, sizeName).takeOwnership(this));
        Workloads.repeat(batchLists.getFirst(), nLists-1, batchLists, this);
        vars = Workloads.makeVars(batchLists.getFirst());
    }

    @TearDown(Level.Trial) public void tearDown() {
        ropeTypeHolder.close();
        for (Batch b : batchLists)
            b.recycle(this);
    }

    @Benchmark public <S extends ResultsSerializer<S>> Rope serialize() {
        try (var serializerGuard = new Guard<S>(this)) {
            var serializer = serializerGuard.set((Orphan<S>)ResultsSerializer.create(format));
            ByteSink sink = ropeTypeHolder.byteSink();
            serializer.init(vars, vars, false);
            serializer.serializeHeader(sink.touch());
            nextBatchList = (nextBatchList+1) % batchLists.size();
            serializer.serialize(batchLists.get(nextBatchList), this, sink);
            serializer.serializeTrailer(sink);
            return ropeTypeHolder.takeRope(sink);
        }
    }
}
