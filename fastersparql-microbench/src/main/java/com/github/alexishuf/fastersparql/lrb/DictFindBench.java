package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.store.index.Dict;
import com.github.alexishuf.fastersparql.store.index.HdtConverter;
import org.openjdk.jmh.annotations.*;

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
@Measurement(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class DictFindBench {
    @Param({"23"}) private int seed;

    private Path full;
    private Path dir;
    private Dict dict;
    private Dict.Lookup lookup;
    private PlainRope[] segmentRopes;
    private PlainRope[] twoSegmentRopes;
    private int nextString = 0;

    @Setup(Level.Trial) public void setup() throws IOException {
        var hdtPath = Files.createTempFile("fastersparql-NYT", ".hdt");
        try (var is = HdtBench.class.getResourceAsStream("NYT.hdt");
             var out = new FileOutputStream(hdtPath.toFile())) {
            if (is == null) throw new RuntimeException("Resource NYT.hdt not found");
            if (is.transferTo(out) == 0) throw new RuntimeException("Empty NYT.hdt");
        }
        full = Files.createTempDirectory("fastersparql");
        new HdtConverter().convert(hdtPath, full);
        Random random = new Random(seed);
        try (Dict shared = new Dict(full.resolve("shared"));
             Dict d = new Dict(full.resolve("strings"), shared)) {
            var lookup = d.lookup();
            int n = (int) d.strings();
            segmentRopes = new PlainRope[n];
            twoSegmentRopes = new PlainRope[n];
            List<Integer> ids = new ArrayList<>(IntStream.range(1, n + 1).boxed().toList());
            Collections.shuffle(ids, random);
            for (int i = 0; i < n; i++) {
                var r = lookup.get(ids.get(i).longValue());
                assert r != null;
                twoSegmentRopes[i] = r;
                segmentRopes[i] = new ByteRope(r.len).append(r);
            }
        }
    }

    @TearDown(Level.Trial)  public void tearDown() throws IOException {
        try (var files = Files.newDirectoryStream(full)) {
            for (Path file : files)
                Files.deleteIfExists(file);
        }
        Files.deleteIfExists(full);
    }

    @Setup(Level.Iteration) public void iterationSetup() throws IOException {
        Path shared = full.resolve("shared"), strings = full.resolve("strings");
        dir = Files.createTempDirectory("fastersparql");
        Path sharedCopy = dir.resolve("shared"), stringsCopy = dir.resolve("strings");
        Files.copy(shared, sharedCopy);
        Files.copy(strings, stringsCopy);
        Dict sharedDict = new Dict(sharedCopy);
        dict = new Dict(stringsCopy, sharedDict);
        lookup = dict.lookup();
    }

    @TearDown(Level.Iteration) public void iterationTearDown() throws IOException {
        dict.close();
        try (var paths = Files.newDirectoryStream(dir)) {
            for (Path path : paths) Files.deleteIfExists(path);
        }
        Files.deleteIfExists(dir);
    }

    private long findAll(PlainRope[] strings) {
        Dict.Lookup lookup = this.lookup;
        long xor = 0;
        int i = nextString+10 > strings.length ? 0 : nextString;
        for (int end = i+10; i < end; ++i)
            xor ^= lookup.find(strings[i]);
        nextString = i;
        return xor;
    }

    @Benchmark public long           find() { return findAll(segmentRopes); }
    @Benchmark public long findTwoSegment() { return findAll(twoSegmentRopes); }
}
