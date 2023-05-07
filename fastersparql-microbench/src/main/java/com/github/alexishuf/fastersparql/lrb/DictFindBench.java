package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.store.index.*;
import com.github.alexishuf.fastersparql.util.IOUtils;
import org.openjdk.jmh.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Thread)
@Threads(1)
@Fork(value = 1, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(iterations = 7, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class DictFindBench {
    private static final Logger log = LoggerFactory.getLogger(DictFindBench.class);

    public enum RopeType {
        SEGMENT,
        TWO_SEGMENT
    }

    @Param({"23"}) private int seed;
    @Param({"1.0", "0.2", "0.01"}) private double queryProportion;
    @Param({"false", "true"}) private boolean standalone;
    @Param({"false", "true"}) private boolean optimizeLocality;
    @Param({"SEGMENT", "TWO_SEGMENT"}) private RopeType ropeType;

    private Path sourceDir, currentDir;
    private Dict dict;
    private Dict.AbstractLookup lookup;

    private PlainRope[] queries;
    private long expected;

    @Setup(Level.Trial) public void setup() throws IOException {
        long setupStart = System.nanoTime();
        var hdtPath = Files.createTempFile("fastersparql-NYT", ".hdt");
        try (var is = HdtBench.class.getResourceAsStream("NYT.hdt");
             var out = new FileOutputStream(hdtPath.toFile())) {
            if (is == null) throw new RuntimeException("Resource NYT.hdt not found");
            if (is.transferTo(out) == 0) throw new RuntimeException("Empty NYT.hdt");
        }
        sourceDir = Files.createTempDirectory("fastersparql");
        new HdtConverter().optimizeLocality(optimizeLocality).standaloneDict(standalone)
                .convert(hdtPath, sourceDir);

        Random random = new Random(seed);
        int nStrings;
        try (var d = Dict.load(sourceDir.resolve("strings"))) {
            var lookup = d.polymorphicLookup();
            nStrings = (int) d.strings();
            int nQueries = (int) Math.max(1, nStrings*queryProportion);

            queries = new PlainRope[nQueries];
            List<Integer> ids = new ArrayList<>(IntStream.range(1, nQueries + 1).boxed().toList());
            Collections.shuffle(ids, random);
            Splitter split = new Splitter(Splitter.Mode.LAST);
            for (int i = 0; i < nQueries; i++) {
                var r = lookup.get(ids.get(i).longValue());
                assert r != null;
                queries[i] = switch (ropeType) {
                    case SEGMENT -> new ByteRope(r.len).append(r);
                    case TWO_SEGMENT -> {
                        ByteRope fst = new ByteRope(r.len), snd = new ByteRope(r.len);
                        switch (split.split(r)) {
                            case PREFIX      -> { fst.append(split.shared()); snd.append(split.local()); }
                            case SUFFIX,NONE ->  { fst.append(split.local()); snd.append(split.shared()); }
                        }
                        var tsr = new TwoSegmentRope();
                        tsr.wrapFirst(fst);
                        tsr.wrapSecond(snd);
                        yield tsr;
                    }
                };
            }
        }
        long us = System.nanoTime()-setupStart/1_000;
        log.info("setup in {}.{}ms, queries={}/{}. fsync()ing...",
                 us/1_000, us%1_000, queries.length, nStrings);
        IOUtils.fsync(1_000);
    }

    private static void deleteDir(Path dir) throws IOException {
        try (var files = Files.newDirectoryStream(dir)) {
            for (Path file : files)
                Files.deleteIfExists(file);
        }
        Files.deleteIfExists(dir);
        IOUtils.fsync(1_000);
    }

    @TearDown(Level.Trial)  public void tearDown() throws IOException {
        deleteDir(sourceDir);
    }

    @Setup(Level.Invocation) public void invocationSetup() throws IOException {
        Path shared = sourceDir.resolve("shared"), strings = sourceDir.resolve("strings");
        currentDir = Files.createTempDirectory("fastersparql");
        Path sharedCopy = currentDir.resolve("shared"), stringsCopy = currentDir.resolve("strings");
        if (Files.exists(shared))
            Files.copy(shared, sharedCopy);
        Files.copy(strings, stringsCopy);
        IOUtils.fsync(10_000);
        lookup = (dict = Dict.load(currentDir.resolve("strings"))).polymorphicLookup();
    }

    @TearDown(Level.Invocation) public void invocationTearDown() throws IOException {
        dict.close();
        deleteDir(currentDir);
    }

    long checkXor(long xor) {
        if (expected == 0)
            expected = xor;
        else if (expected != xor)
            throw new RuntimeException("xor="+xor+" changed since last benchmark ("+expected+")");
        return xor;
    }

    private long find(SortedStandaloneDict.Lookup lookup) {
        long xor = 0;
        for (var r : queries)
            xor ^= lookup.find(r);
        return checkXor(xor);
    }
    private long find(LocalityStandaloneDict.Lookup lookup) {
        long xor = 0;
        for (var r : queries)
            xor ^= lookup.find(r);
        return checkXor(xor);
    }
    private long find(SortedCompositeDict.Lookup lookup) {
        long xor = 0;
        for (var r : queries)
            xor ^= lookup.find(r);
        return checkXor(xor);
    }
    private long find(LocalityCompositeDict.Lookup lookup) {
        long xor = 0;
        for (var r : queries)
            xor ^= lookup.find(r);
        return checkXor(xor);
    }

    @Benchmark public long find() {
        if      (lookup instanceof SortedStandaloneDict.Lookup         l) return find(l);
        else if (lookup instanceof LocalityStandaloneDict.Lookup l) return find(l);
        else if (lookup instanceof SortedCompositeDict.Lookup          l) return find(l);
        else if (lookup instanceof LocalityCompositeDict.Lookup  l) return find(l);
        throw new UnsupportedOperationException("Unexpected dict type");
    }
}
