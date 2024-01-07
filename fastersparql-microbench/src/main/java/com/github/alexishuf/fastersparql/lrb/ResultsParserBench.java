package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.results.ResultsParser;
import com.github.alexishuf.fastersparql.sparql.results.ResultsSender;
import com.github.alexishuf.fastersparql.sparql.results.WsClientParser;
import com.github.alexishuf.fastersparql.sparql.results.WsFrameSender;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.sparql.results.serializer.WsSerializer;
import com.github.alexishuf.fastersparql.util.concurrent.Unparker;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.github.alexishuf.fastersparql.sparql.results.ResultsParser.createFor;

@SuppressWarnings({"rawtypes", "unchecked"})
@State(Scope.Thread)
@Threads(1)
@Fork(value = 1, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 4, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ResultsParserBench {
    private static final int FRAGMENTS_LISTS = 64;

    @Param({"COMPRESSED", "TERM"}) private String typeName;
    @Param({"ALL"})                private String sizeName;
    @Param({"TSV", "JSON", "WS"})  private SparqlResultFormat format;
    @Param({"NETTY", "BYTE"})      private RopeType ropeType;
    @Param({"16384"})              private int maxItems;

    private Blackhole blackhole;
    private BatchType bt;
    private Vars vars;
    private RopeTypeHolder ropeTypeHolder;
    private List<List<SegmentRope>> fragmentsLists;
    private int nextFragmentList = 0;
    private volatile boolean stopDrainer = false;
    private volatile int drainedRows = -1;
    private volatile BIt<? extends Batch> it = null;
    private Thread drainer;
    private final NopWsFrameSender wsFrameSender = new NopWsFrameSender();

    @SuppressWarnings("unchecked") @Setup(Level.Trial) public void setup() {
        long start = Timestamp.nanoTime();
        bt = Workloads.parseBatchType(typeName);
        ropeTypeHolder = new RopeTypeHolder(ropeType);
        System.out.printf("Loading %s batches...\n", sizeName);
        List<Batch> batches = Workloads.uniformCols(Workloads.<Batch>fromName(bt, sizeName), bt);
        vars = Workloads.makeVars(batches);

        System.out.printf("Serializing fragments %d times...\n", FRAGMENTS_LISTS);
        long last = Timestamp.nanoTime();
        fragmentsLists = new ArrayList<>(FRAGMENTS_LISTS);
        nextFragmentList = 0;
        ByteSink sink = ropeTypeHolder.byteSink();
        var serializer = ResultsSerializer.create(format);
        for (int i = 0; i < FRAGMENTS_LISTS; i++) {
            List<SegmentRope> fragments = new ArrayList<>();
            serializer.init(vars, vars, false, sink.touch());
            fragments.add(ropeTypeHolder.takeRope(sink));
            for (Batch b : batches) {
                serializer.serializeAll(b, sink.touch());
                fragments.add(ropeTypeHolder.takeRope(sink));
            }
            serializer.serializeTrailer(sink.touch());
            fragments.add(ropeTypeHolder.takeRope(sink));
            fragmentsLists.add(fragments);
            if (Timestamp.nanoTime()-last > 5_000_000_000L) {
                last = Timestamp.nanoTime();
                System.out.printf("Serializing fragments: %d/%d...\n", i, FRAGMENTS_LISTS);
            }
            if ((i&31) == 0)
                System.gc();
        }
        sink.release();
        Integer listBytes = fragmentsLists.getFirst().stream().map(Rope::len).reduce(Integer::sum).orElse(0);
        System.out.printf("Serialized fragments in %.3fms. Built %d lists with %d fragments each, " +
                          "totaling %.3f KiB per list. Will gc() and cooldown sleep(500)\n",
                          (Timestamp.nanoTime()-start)/1_000_000.0, fragmentsLists.size(),
                          fragmentsLists.getFirst().size(), listBytes/1024.0);
        drainer = Thread.ofPlatform().name("drainer").start(this::drain);
        Workloads.cooldown(500);
    }

    @TearDown(Level.Trial) public void trialTearDown() throws InterruptedException {
        stopDrainer = true;
        Unparker.unpark(drainer);
        ropeTypeHolder.close();
        drainer.join();
    }

    private static class NopWsFrameSender<S extends ByteSink<S, T>, T>
            implements WsFrameSender<S, T>{
        @Override public void sendFrame(T content) {}
        @Override public S createSink() {return null;}

        @Override public ResultsSender<S, T> createSender() {
            return new ResultsSender<>(WsSerializer.create(), null) {
                @Override public void preTouch() {}
                @Override public void sendInit(Vars vars, Vars subset, boolean isAsk) {}
                @Override public void sendSerializedAll(Batch<?> batch) {}
                @Override public void sendSerialized(Batch<?> batch, int from, int nRows) {}
                @Override public void sendTrailer() {}
                @Override public void sendError(Throwable cause) {}
                @Override public void sendCancel() {}
            };
        }
    }

    @SuppressWarnings("unchecked") private <B extends Batch<B>>void drain() {
        while (!stopDrainer) {
            while (!stopDrainer && it == null)
                LockSupport.park();
            if (it == null) {
                if (stopDrainer) break;
                else             continue;
            }
            BIt<B> it = (BIt<B>) this.it;
            var blackhole = this.blackhole;
            int rows = 0;
            for (B b = null; (b = it.nextBatch(b)) != null; ) {
                for (var n = b; n != null; n = n.next) {
                    rows += n.rows;
                    blackhole.consume(n);
                }
            }
            drainedRows = rows;
        }
    }

    private int waitDrainer() {
        // busy wait in a dedicated method avoids this wait being confused waits at production code
        while (drainedRows < 0) Thread.onSpinWait();
        return drainedRows;
    }

    private int parse(ResultsParser parser, BIt it) {
        this.drainedRows = -1;
        this.it = it;
        Unparker.unpark(drainer);
        List<SegmentRope> fragments = fragmentsLists.get(nextFragmentList);
        nextFragmentList = (nextFragmentList+1) % fragmentsLists.size();
        try {
            for (var fragment : fragments)
                parser.feedShared(fragment);
            parser.feedEnd();
        } catch (TerminatedException|CancelledException ignored) {}
        return waitDrainer();
    }

    private ResultsParser createParser(CallbackBIt delegate) {
        if (format != SparqlResultFormat.WS) return createFor(format, delegate);
        WsClientParser parser = new WsClientParser(delegate);
        parser.setFrameSender(wsFrameSender);
        return parser;
    }

    @Benchmark public int parse(Blackhole blackhole) {
        this.blackhole = blackhole;
        SPSCBIt delegate = new SPSCBIt<>(bt, vars, maxItems);
        var parser = createParser(delegate);
        return parse(parser, delegate);
    }
}

