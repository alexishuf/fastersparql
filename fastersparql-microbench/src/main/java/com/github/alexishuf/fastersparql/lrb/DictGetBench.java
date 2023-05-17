package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.hdt.batch.IdAccess;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.store.batch.IdTranslator;
import com.github.alexishuf.fastersparql.store.index.HdtConverter;
import com.github.alexishuf.fastersparql.store.index.dict.*;
import com.github.alexishuf.fastersparql.util.IOUtils;
import org.openjdk.jmh.annotations.*;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
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

import static com.github.alexishuf.fastersparql.store.batch.IdTranslator.*;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

@State(Scope.Thread)
@Threads(1)
@Fork(value = 1, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(iterations = 7, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class DictGetBench {
    private static final Logger log = LoggerFactory.getLogger(DictGetBench.class);


    @Param({"23"}) private int seed;
    @Param({"1.0", "0.2", "0.01"}) private double queryProportion;
    @Param({"false", "true"}) private boolean standalone;
    @Param({"false", "true"}) private boolean optimizeLocality;
    @Param({"false"}) private boolean testHdt;

    private Path sourceDir, currentDir;
    private Dict dict;
    private Dict.AbstractLookup lookup;
    private int dictId;
    private Path hdtSource;
    private HDT hdt;
    private Dictionary hdtDict;
    private int hdtDictId;

    private int[] queries;
    private int[] hdtQueries;
    private TripleComponentRole[] hdtRoles;

    @Setup(Level.Trial) public void setup() throws IOException {
        long setupStart = System.nanoTime();
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

        try (var d = Dict.load(sourceDir.resolve("strings"));
             HDT hdt = HDTManager.mapHDT(hdtSource.toString())) {
            nStrings = (int) Math.min(Integer.MAX_VALUE>>2, d.strings());
            int nQueries = (int) Math.max(1, nStrings*queryProportion);

            List<Integer> ids = new ArrayList<>(IntStream.range(1, nStrings + 1).boxed().toList());
            Collections.shuffle(ids, random);
            queries = ids.stream().limit(nQueries).mapToInt(Integer::intValue).toArray();

            Dictionary hd = hdt.getDictionary();
            hdtQueries = new int[nQueries];
            hdtRoles = new TripleComponentRole[nQueries];
            int sharedMax = (int)hd.getNshared();
            int subjectMax = sharedMax + (int)hd.getSubjects().getNumberOfElements();
            int objectsMax = subjectMax+ (int)hd.getObjects().getNumberOfElements();
            int predicatesMax = objectsMax+ (int)hd.getNpredicates();
            for (int i = 0; i < queries.length; i++) {
                int id = queries[i];
                if (id <= subjectMax) {
                    hdtQueries[i] = id;
                    hdtRoles[i] = TripleComponentRole.SUBJECT;
                } else if (id <= objectsMax) {
                    hdtQueries[i] = id-subjectMax;
                    hdtRoles[i] = TripleComponentRole.OBJECT;
                } else if (id <= predicatesMax) {
                    hdtQueries[i] = id-objectsMax;
                    hdtRoles[i] = TripleComponentRole.PREDICATE;
                } else {
                    throw new AssertionError();
                }
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
        if (dict instanceof LocalityCompositeDict lcd)
            dictId = IdTranslator.register(lcd);

        if (testHdt) {
            Path hdtCopy = currentDir.resolve("NYT.hdt");
            Files.copy(hdtSource, hdtCopy);
            hdt = HDTManager.mapHDT(hdtCopy.toString());
            hdtDict = hdt.getDictionary();
            hdtDictId = IdAccess.register(hdtDict);
        }
    }

    @TearDown(Level.Invocation) public void invocationTearDown() throws IOException {
        if (dictId != 0) {
            IdTranslator.deregister(dictId, dict);
            dictId = 0;
        }
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

    private int get(SortedStandaloneDict.Lookup l) {
        int acc = 0;
        SegmentRope tmp = new SegmentRope();
        for (int id : queries) {
            //noinspection DataFlowIssue
            tmp.wrap(l.get(id));
            acc += tmp.len;
        }
        return acc;
    }

    private int get(LocalityStandaloneDict.Lookup l) {
        int acc = 0;
        SegmentRope tmp = new SegmentRope();
        for (int id : queries) {
            //noinspection DataFlowIssue
            tmp.wrap(l.get(id));
            acc += tmp.len;
        }
        return acc;
    }

    private int get(SortedCompositeDict.Lookup l) {
        int acc = 0;
        var tmp = new TwoSegmentRope();
        for (int id : queries) {
            //noinspection DataFlowIssue
            tmp.shallowCopy(l.get(id));
            acc += tmp.len;
        }
        return acc;
    }

    private int get(LocalityCompositeDict.Lookup l) {
        int acc = 0;
        var tmp = new TwoSegmentRope();
        for (int id : queries) {
            //noinspection DataFlowIssue
            tmp.shallowCopy(l.get(id));
            acc += tmp.len;
        }
        return acc;
    }

    private int getHdt(Dictionary d) {
        int acc = 0;
        for (int i = 0; i < hdtQueries.length; i++)
            acc += d.idToString(hdtQueries[i], hdtRoles[i]).length();
        return acc;
    }

    @Benchmark public int id2string() {
        if (testHdt) return getHdt(hdtDict);
        else if (lookup instanceof SortedStandaloneDict.Lookup   l) return get(l);
        else if (lookup instanceof LocalityStandaloneDict.Lookup l) return get(l);
        else if (lookup instanceof SortedCompositeDict.Lookup    l) return get(l);
        else if (lookup instanceof LocalityCompositeDict.Lookup  l) return get(l);
        throw new UnsupportedOperationException();
    }

    @Benchmark public int id2term() {
        int acc = 0;
        if (testHdt) {
            for (int i = 0; i < hdtQueries.length; i++) {
                long sourced = IdAccess.encode(hdtQueries[i], hdtDictId, hdtRoles[i]);
                //noinspection DataFlowIssue
                acc += IdAccess.toTerm(sourced).len;
            }
        } else {
            for (int id : queries) {
                TwoSegmentRope tsr = lookup(dictId).get(id & ID_MASK);
                if (tsr == null) throw new NullPointerException();
                SegmentRope shared = new SegmentRope(tsr.fst, tsr.fstOff, tsr.fstLen);
                SegmentRope local = new SegmentRope(tsr.snd, tsr.sndOff, tsr.sndLen);
                boolean isLit = tsr.fstLen > 0 && tsr.fst.get(JAVA_BYTE, 0) == '"';
                if (isLit) {
                    var tmp = shared;
                    shared = local;
                    local = tmp;
                }
                acc += new Term(shared, local, isLit).len;
            }
        }
        return acc;
    }

}
