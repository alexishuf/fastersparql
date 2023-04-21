package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.results.ResultsParserBIt;
import com.github.alexishuf.fastersparql.sparql.results.WsClientParserBIt;
import com.github.alexishuf.fastersparql.sparql.results.WsFrameSender;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.github.alexishuf.fastersparql.sparql.results.ResultsParserBIt.createFor;

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
    private List<List<Rope>> fragmentsLists;
    private int nextFragmentList = 0;
    private volatile boolean stopDrainer = false;
    private volatile int drainedRows = -1;
    private volatile BIt<? extends Batch> it = null;
    private Thread drainer;
    private final NopWsFrameSender wsFrameSender = new NopWsFrameSender();

    @SuppressWarnings("unchecked") @Setup(Level.Trial) public void setup() {
        long start = System.nanoTime();
        bt = Workloads.parseBatchType(typeName);
        ropeTypeHolder = new RopeTypeHolder(ropeType);
        System.out.printf("Loading %s batches...\n", sizeName);
        List<Batch> batches = Workloads.uniformCols(Workloads.<Batch>fromName(bt, sizeName), bt);
        vars = Workloads.makeVars(batches);

        System.out.printf("Serializing fragments %d times...\n", FRAGMENTS_LISTS);
        long last = System.nanoTime();
        fragmentsLists = new ArrayList<>(FRAGMENTS_LISTS);
        nextFragmentList = 0;
        var serializer = ResultsSerializer.create(format);
        for (int i = 0; i < FRAGMENTS_LISTS; i++) {
            List<Rope> fragments = new ArrayList<>();
            ByteSink sink = ropeTypeHolder.byteSink();
            serializer.init(vars, vars, false, sink);
            fragments.add(ropeTypeHolder.takeRope(sink));
            for (Batch b : batches) {
                serializer.serialize(b, sink = ropeTypeHolder.byteSink());
                fragments.add(ropeTypeHolder.takeRope(sink));
            }
            serializer.serializeTrailer(sink = ropeTypeHolder.byteSink());
            fragments.add(ropeTypeHolder.takeRope(sink));
            fragmentsLists.add(fragments);
            if (System.nanoTime()-last > 5_000_000_000L) {
                last = System.nanoTime();
                System.out.printf("Serializing fragments: %d/%d...\n", i, FRAGMENTS_LISTS);
            }
            if ((i&31) == 0)
                System.gc();
        }
        Integer listBytes = fragmentsLists.get(0).stream().map(Rope::len).reduce(Integer::sum).orElse(0);
        System.out.printf("Serialized fragments in %.3fms. Built %d lists with %d fragments each, " +
                          "totaling %.3f KiB per list. Will gc() and cooldown sleep(500)\n",
                          (System.nanoTime()-start)/1_000_000.0, fragmentsLists.size(),
                          fragmentsLists.get(0).size(), listBytes/1024.0);
        drainer = Thread.ofPlatform().name("drainer").start(this::drain);
        Workloads.cooldown(500);
    }

    @TearDown(Level.Trial) public void trialTearDown() throws InterruptedException {
        stopDrainer = true;
        LockSupport.unpark(drainer);
        ropeTypeHolder.close();
        drainer.join();
    }

    private static class NopWsFrameSender<S extends ByteSink<S>> implements WsFrameSender<S>{
        @Override public void sendFrame(S content) {}
        @Override public S createSink() {return null;}
        @Override public void releaseSink(S sink) {}
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
                rows += b.rows;
                blackhole.consume(b);
            }
            drainedRows = rows;
        }
    }

    private int waitDrainer() {
        // busy wait in a dedicated method avoids this wait being confused waits at production code
        while (drainedRows < 0) Thread.onSpinWait();
        return drainedRows;
    }

    private int parse(ResultsParserBIt parser, BIt it) {
        this.drainedRows = -1;
        this.it = it;
        LockSupport.unpark(drainer);
        List<Rope> fragments = fragmentsLists.get(nextFragmentList);
        nextFragmentList = (nextFragmentList+1) % fragmentsLists.size();
        try (parser) {
            for (Rope fragment : fragments)
                parser.feedShared(fragment);
            parser.complete(null);
        }
        return waitDrainer();
    }

    private ResultsParserBIt createParser() {
        return format != SparqlResultFormat.WS ? createFor(format, bt, vars, maxItems)
                : new WsClientParserBIt(wsFrameSender, bt, vars, maxItems);
    }
    private ResultsParserBIt createParser(CallbackBIt delegate) {
        return format != SparqlResultFormat.WS ? createFor(format, delegate)
                : new WsClientParserBIt(wsFrameSender, delegate);
    }

    @Benchmark public int parse(Blackhole blackhole) {
        this.blackhole = blackhole;
        var parser = createParser();
        return parse(parser, parser);
    }

    @Benchmark public int parseDelegate(Blackhole blackhole) {
        this.blackhole = blackhole;
        SPSCBIt delegate = new SPSCBIt<>(bt, vars, maxItems);
        var parser = createParser(delegate);
        return parse(parser, delegate);
    }
}

