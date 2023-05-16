package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.hdt.batch.IdAccess;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.store.index.dict.Dict;
import com.github.alexishuf.fastersparql.store.index.triples.Triples;
import com.github.alexishuf.fastersparql.store.index.triples.TriplesSorter;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.github.alexishuf.fastersparql.client.util.TestTaskSet.platformTaskSet;
import static com.github.alexishuf.fastersparql.store.index.dict.Dict.NOT_FOUND;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.*;
import static org.rdfhdt.hdt.enums.TripleComponentRole.*;

public class IterationTester implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(IterationTester.class);
    private final Path dir;
    private final Triples spo, pso, ops;
    private final @Nullable Dict strings;
    private final List<TestTriple> allTriples;
    private final List<TestTriple> triples;
    private final int[] terms;
    private static final ThreadLocal<ArrayList<TestTriple>> exTL = ThreadLocal.withInitial(ArrayList::new);
    private static final ThreadLocal<ArrayList<TestTriple>> acTL = ThreadLocal.withInitial(ArrayList::new);

    private IterationTester(Path dir, List<TestTriple> allTriples,
                            @Nullable Dict strings, Triples spo, Triples pso, Triples ops) {
        this.dir = dir;
        this.strings = strings;
        this.spo = spo;
        this.pso = pso;
        this.ops = ops;

        List<TestTriple> triples = allTriples;
        if (allTriples.size() > 20)
            triples = capTriples(allTriples, 20);

        this.allTriples = allTriples;
        this.triples = triples;
        BitSet is = new BitSet();
        for (TestTriple(long s, long p, long o) : triples) {
            is.set((int) s);  is.set((int) p);  is.set((int) o);
        }
        terms = new int[is.cardinality()];
        int out = 0;
        for (int i = 0; (i = is.nextSetBit(i)) >= 0; ++i) terms[out++] = i;
        assert out == terms.length;
    }

    public static IterationTester create(List<TestTriple> triples) throws IOException {
        var tempDir = Files.createTempDirectory("fastersparql");
        try (var sorter = new TriplesSorter(tempDir)) {
            for (var t : triples)  t.addTo(sorter);
            sorter.write(tempDir);
        } catch (Throwable t) {
            try {
                Files.deleteIfExists(tempDir);
            } catch (IOException ignored) {}
            throw t;
        }
        return load(tempDir, triples);
    }

    public static IterationTester
    createFromHDT(Class<?> ref, String resourcePath, HdtConverter converter) throws IOException {
        var tempDir = Files.createTempDirectory("fastersparql");
        Path hdtPath = tempDir.resolve("origin.hdt");
        List<TestTriple> triples;
        try {
            try (var is = ref.getResourceAsStream(resourcePath);
                 var out = new FileOutputStream(hdtPath.toFile())) {
                assertNotNull(is);
                is.transferTo(out);
            }
            converter.convert(hdtPath, tempDir);
            triples = triplesFromHDT(tempDir);
        } catch (Throwable t) {
            deleteDir(tempDir);
            throw t;
        }
        return load(tempDir, triples);
    }

    public static List<TestTriple> capTriples(List<TestTriple> triples, int max) {
        int size = triples.size();
        if (size < max)
            return triples;
        Random r = new Random(23);
        BitSet selected = new BitSet();
        while (selected.cardinality() < max)
            selected.set(r.nextInt(size));
        ArrayList<TestTriple> subset = new ArrayList<>();
        for (int i = 0; (i = selected.nextSetBit(i)) >= 0; ++i)
            subset.add(triples.get(i));
        return subset;
    }

    public static List<TestTriple> triplesFromHDT(Path dir) throws IOException {
        List<TestTriple> triples = new ArrayList<>();
        int dictId = 0;
        try (var strings = Dict.load(dir.resolve("strings"));
             HDT hdt = HDTManager.mapHDT(dir.resolve("origin.hdt").toString())) {
            var lookup = strings.polymorphicLookup();
            dictId = IdAccess.register(hdt.getDictionary());
            for (var it = hdt.getTriples().searchAll(); it.hasNext(); ) {
                TripleID triple = it.next();
                var sNT = IdAccess.toNT(IdAccess.encode(triple.getSubject(),   dictId, SUBJECT));
                var pNT = IdAccess.toNT(IdAccess.encode(triple.getPredicate(), dictId, PREDICATE));
                var oNT = IdAccess.toNT(IdAccess.encode(triple.getObject(),    dictId, OBJECT));
                if (sNT == null || pNT == null || oNT == null)
                    fail("Buggy IdAccess");
                long s = lookup.find(sNT);
                long p = lookup.find(pNT);
                long o = lookup.find(oNT);
                if (s == NOT_FOUND || p == NOT_FOUND || o == NOT_FOUND)
                    fail("Terms missing from Dict");
                triples.add(new TestTriple(s, p, o));
            }

        } finally {
            if (dictId != 0) IdAccess.release(dictId);
        }
        return triples;
    }

    private static IterationTester load(Path dir, List<TestTriple> triples) throws IOException {
        Dict strings = null;
        Triples spo = null, pso = null, ops = null;
        try {
            Path stringsFile = dir.resolve("strings");
            if (Files.exists(stringsFile))
                strings = Dict.load(stringsFile);
            spo = new Triples(dir.resolve("spo"));
            pso = new Triples(dir.resolve("pso"));
            ops = new Triples(dir.resolve("ops"));
            return new IterationTester(dir, triples, strings, spo, pso, ops);
        } catch (Throwable t) {
            if (spo != null) spo.close();
            if (pso != null) pso.close();
            if (ops != null) ops.close();
            if (strings != null) strings.close();
            throw t;
        }
    }

    @Override public void close() throws IOException {
        spo.close();
        pso.close();
        ops.close();
        if (strings != null) strings.close();
        if (dir != null)
            deleteDir(dir);
    }

    private static void deleteDir(Path dir) throws IOException {
        List<Path> notDeleted = new ArrayList<>();
        try (var paths = Files.newDirectoryStream(dir)) {
            for (Path child : paths) {
                try {
                    Files.deleteIfExists(child);
                } catch (IOException e) { notDeleted.add(child); }
            }
        }
        if (!notDeleted.isEmpty()) {
            String plain = notDeleted.stream().map(Path::toString).collect(joining(", "));
            throw new IOException("Failed to delete some files: "+ plain);
        }
    }

    @FunctionalInterface interface KeyPredicate {
        boolean test(long key, TestTriple triple);
    }

    @FunctionalInterface interface KeySubKeyPredicate {
        boolean test(long key, long subKey, TestTriple triple);
    }

    @FunctionalInterface interface TripleFactory {
        TestTriple create(long key, long subKey, long value);
    }

    public void testLoadAndLookupStrings() {
        assertNotNull(strings);
        var lookup = strings.polymorphicLookup();
        for (int id : terms) {
            PlainRope string = lookup.get(id);
            assertNotNull(string);
            assertEquals(id, lookup.find(string));
        }
    }

    public void testIteration() throws Exception {
        if (triples == allTriples) {
            testScan(spo, TestTriple::new);
            testScan(pso, (p, s, o) -> new TestTriple(s, p, o));
            testScan(ops, (o, p, s) -> new TestTriple(s, p, o));

            testPairs((k, t) -> t.s() == k, spo, TestTriple::new);
            testPairs((k, t) -> t.p() == k, pso, (p, s, o) -> new TestTriple(s, p, o));
            testPairs((k, t) -> t.o() == k, ops, (o, p, s) -> new TestTriple(s, p, o));

            testContains();

            testValues((k, sk, t) -> t.s() == k && t.p() == sk, spo, TestTriple::new);
            testValues((k, sk, t) -> t.p() == k && t.s() == sk, pso, (p, s, o) -> new TestTriple(s, p, o));
            testValues((k, sk, t) -> t.o() == k && t.p() == sk, ops, (o, p, s) -> new TestTriple(s, p, o));

            testSubKeys((k, v, t) -> t.s() == k && t.o() == v, spo, (s, o, p) -> new TestTriple(s, p, o));
            testSubKeys((k, v, t) -> t.p() == k && t.o() == v, pso, (p, o, s) -> new TestTriple(s, p, o));
            testSubKeys((k, v, t) -> t.o() == k && t.s() == v, ops, (o, s, p) -> new TestTriple(s, p, o));
        } else {
            log.info("this test will take >30s....");
            try (TestTaskSet tasks = platformTaskSet(getClass().getSimpleName())) {
                tasks.add(() -> testPairs((k, t) -> t.s() == k, spo, TestTriple::new));
                tasks.add(this::testContains);
                tasks.add(() -> testValues((k, sk, t) -> t.p() == k && t.s() == sk, pso, (p, s, o) -> new TestTriple(s, p, o)));
                tasks.add(() -> testSubKeys((k, v, t) -> t.o() == k && t.s() == v, ops, (o, s, p) -> new TestTriple(s, p, o)));
            }
        }
    }

    private @Nullable List<TestTriple> initEx(int key, KeyPredicate predicate) {
        ArrayList<TestTriple> ex = exTL.get();
        ex.clear();
        int max = triples.size();
        for (TestTriple t : allTriples) {
            if (predicate.test(key, t)) ex.add(t);
            if (ex.size() == max) return null;
        }
        return ex;
    }

    private @Nullable List<TestTriple> initEx(int key, int subKey, KeySubKeyPredicate predicate) {
        ArrayList<TestTriple> ex = exTL.get();
        ex.clear();
        int max = triples.size();
        for (TestTriple t : allTriples) {
            if (predicate.test(key, subKey, t)) ex.add(t);
            if (ex.size() == max) return null;
        }
        return ex;
    }

    private void testScan(Triples index, TripleFactory factory) {
        var ac = acTL.get();
        ac.clear();
        for (var it = index.scan(); it.advance(); )
            ac.add(factory.create(it.keyId, it.subKeyId, it.valueId));
        var ex = exTL.get();
        ex.clear();
        ex.addAll(triples);
        Collections.sort(ex);
        Collections.sort(ac);
        assertEquals(ex, ac);
    }

    private void testPairs(KeyPredicate exPredicate, Triples index, TripleFactory factory) {
        ArrayList<TestTriple> ac = acTL.get();

        for (int k : terms) {
            var ex = initEx(k, exPredicate);
            if (ex == null)
                continue;
            ac.clear();
            for (var it = index.pairs(k); it.advance(); )
                ac.add(factory.create(k, it.subKeyId, it.valueId));
            Collections.sort(ex);
            Collections.sort(ac);
            assertEquals(ex, ac);
        }
    }

    private void testValues(KeySubKeyPredicate exPredicate, Triples index, TripleFactory factory) {
        var ac = acTL.get();
        for (int k : terms) {
            for (int sk : terms) {
                var ex = initEx(k, sk, exPredicate);
                if (ex == null)
                    continue;
                ac.clear();
                for (var it = index.values(k, sk); it.advance(); )
                    ac.add(factory.create(k, sk, it.valueId));
                Collections.sort(ex);
                Collections.sort(ac);
                assertEquals(ex, ac);
//                if (ac.size() != ex.size())
//                    fail("Triple count mismatch: ex="+ex+", ac="+ ac);
//                if (!ex.containsAll(ac))
//                    fail("Unexpected triples: ex="+ex+", ac="+ ac);
//                if (!ac.containsAll(ex))
//                    fail("Missing triples: ex="+ex+", ac="+ ac);
            }
        }
    }

    private void testSubKeys(KeySubKeyPredicate exPredicate, Triples index, TripleFactory factory) {
        var ac = acTL.get();
        for (int k : terms) {
            for (int v : terms) {
                var ex = initEx(k, v, exPredicate);
                if (ex == null)
                    continue;
                ac.clear();
                for (var it = index.subKeys(k, v); it.advance(); )
                    ac.add(factory.create(k, v, it.subKeyId));
                Collections.sort(ex);
                Collections.sort(ac);
                assertEquals(ex, ac);
            }
        }
    }

    private void testContains() {
        for (TestTriple(long s, long p, long o): triples) {
            assertTrue(spo.contains(s, p, o));
            assertTrue(pso.contains(p, s, o));
            assertTrue(ops.contains(o, p, s));
        }
    }
}
