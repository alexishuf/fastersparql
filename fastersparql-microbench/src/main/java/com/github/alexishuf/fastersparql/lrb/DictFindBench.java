package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.hdt.batch.IdAccess;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.store.index.HdtConverter;
import com.github.alexishuf.fastersparql.store.index.dict.*;
import com.github.alexishuf.fastersparql.util.IOUtils;
import org.openjdk.jmh.annotations.*;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.util.string.ReplazableString;
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
    @Param({"0"}) private int testHdt;

    private Path sourceDir, currentDir;
    private Dict dict;
    private Dict.AbstractLookup lookup;
    private Path hdtSource;
    private HDT hdt;
    private Dictionary hdtDict;
    private int hdtDictId;

    private PlainRope[] queries;
    private Term[] hdtTerms;
    private ReplazableString[] hdtQueries;
    private long expected;

    @Setup(Level.Trial) public void setup() throws IOException {
        long setupStart = Timestamp.nanoTime();
        sourceDir = Files.createTempDirectory("fastersparql");
        hdtSource = sourceDir.resolve("NYT.hdt");
        try (var is = HdtBench.class.getResourceAsStream("NYT.hdt");
             var out = new FileOutputStream(hdtSource.toFile())) {
            if (is == null) throw new RuntimeException("Resource NYT.hdt not found");
            if (is.transferTo(out) == 0) throw new RuntimeException("Empty NYT.hdt");
        }
        new HdtConverter().optimizeLocality(optimizeLocality).standaloneDict(standalone)
                          .convert(hdtSource, sourceDir);

        Random random = new Random(seed);
        int nStrings;
        try (var d = Dict.load(sourceDir.resolve("strings"))) {
            var lookup = d.polymorphicLookup();
            nStrings = (int) Math.min(Integer.MAX_VALUE>>2, d.strings());
            int nQueries = (int) Math.max(1, nStrings*queryProportion);

            queries = new PlainRope[nQueries];
            List<Integer> ids = new ArrayList<>(IntStream.range(1, nStrings + 1).boxed().toList());
            Collections.shuffle(ids, random);
            Splitter split = new Splitter(Splitter.Mode.LAST);
            for (int i = 0; i < nQueries; i++) {
                var r = lookup.get(ids.get(i).longValue());
                assert r != null;
                queries[i] = switch (ropeType) {
                    case SEGMENT -> new ByteRope(r.len).append(r);
                    case TWO_SEGMENT -> {
                        TwoSegmentRope tsr = new TwoSegmentRope();
                        boolean flip = split.split(r) == Splitter.SharedSide.SUFFIX;
                        tsr.wrapFirst((SegmentRope)split.shared());
                        tsr.wrapSecond((SegmentRope)split.local());
                        if (flip) tsr.flipSegments();
                        yield tsr;
                    }
                };
            }
            if (testHdt == 1) {
                hdtQueries = new ReplazableString[nQueries];
                for (int i = 0; i < nQueries; i++)
                    hdtQueries[i] = term2hdtLookup(Term.valueOf(queries[i]));
            }
            if (testHdt >= 2) {
                hdtTerms = new Term[nQueries];
                for (int i = 0; i < nQueries; i++)
                    hdtTerms[i] = Term.valueOf(queries[i]);
            }
        }
        long us = Timestamp.nanoTime()-setupStart/1_000;
        log.info("setup in {}.{}ms, queries={}/{}. fsync()ing...",
                 us/1_000, us%1_000, queries.length, nStrings);
        IOUtils.fsync(1_000);
    }

    private static ReplazableString term2hdtLookup(Term t) {

        ReplazableString rs  =switch (t.type()) {
            case LIT -> {
                int endLex = t.endLex(), required = 1 + t.unescapedLexicalSize() + t.len-endLex;
                var str = new ReplazableString(required);
                var unescaped = new ByteRope(str.getBuffer(), 0, 0);
                unescaped.append('"');
                t.unescapedLexical(unescaped);
                unescaped.append(t, endLex, t.len);
                yield str;
            }
            case IRI -> {
                var str = new ReplazableString(t.len - 2);
                t.copy(1, t.len-1, str.getBuffer(), 0);
                yield str;
            }
            default -> {
                var str = new ReplazableString(t.len);
                t.copy(0, t.len, str.getBuffer(), 0);
                yield str;
            }
        };
        byte[] u8 = rs.getBuffer();
        rs.replace(0, u8, 0, u8.length);
        return rs;
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

        if (testHdt > 0) {
            Path hdtCopy = currentDir.resolve("NYT.hdt");
            Files.copy(hdtSource, hdtCopy);
            hdt = HDTManager.mapHDT(hdtCopy.toString());
            hdtDict = hdt.getDictionary();
            hdtDictId = IdAccess.register(hdtDict);
        }
    }

    @TearDown(Level.Invocation) public void invocationTearDown() throws IOException {
        if (hdtDictId != 0)
            IdAccess.release(hdtDictId);
        hdtDictId = 0;
        dict.close();
        if (hdt != null) {
            hdt.close();
            hdt = null;
        }
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

    private long findHdt() {
        long xor = 0;
        Dictionary d = hdtDict;
        for (var q : hdtQueries) {
            long id = d.stringToId(q, TripleComponentRole.SUBJECT);
            if (id <= 0) {
                if ((id = d.stringToId(q, TripleComponentRole.OBJECT)) <= 0)
                    id = d.stringToId(q, TripleComponentRole.PREDICATE);
            }
            xor ^= id;
        }
        return xor;
    }

    private long findHdtUnescape() {
        long xor = 0;
        Dictionary d = hdtDict;
        for (var term : hdtTerms) {
            var q = term2hdtLookup(term);
            long id = d.stringToId(q, TripleComponentRole.SUBJECT);
            if (id <= 0) {
                if ((id = d.stringToId(q, TripleComponentRole.OBJECT)) <= 0)
                    id = d.stringToId(q, TripleComponentRole.PREDICATE);
            }
            xor ^= id;
        }
        return xor;
    }

    @Benchmark public long find() {
        if      (testHdt == 1                                     ) return findHdt();
        else if (testHdt == 2                                     ) return findHdtUnescape();
        else if (lookup instanceof SortedStandaloneDict.Lookup   l) return find(l);
        else if (lookup instanceof LocalityStandaloneDict.Lookup l) return find(l);
        else if (lookup instanceof SortedCompositeDict.Lookup    l) return find(l);
        else if (lookup instanceof LocalityCompositeDict.Lookup  l) return find(l);
        throw new UnsupportedOperationException("Unexpected dict type");
    }

}
