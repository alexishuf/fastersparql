package com.github.alexishuf.fastersparql.store.index.triples;

import com.github.alexishuf.fastersparql.store.index.TestTriple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TriplesBlockTest {
    private static Path tempDir;
    private static Arena arena;

    @BeforeAll static void beforeAll() throws IOException {
        tempDir = Files.createTempDirectory("fastersparql");
        arena = Arena.openShared();
    }

    @AfterAll
    static void afterAll() throws IOException {
        arena.close();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(tempDir)) {
            paths.forEach(p -> {try {Files.deleteIfExists(p);} catch (IOException ignored) {}});
        }
        Files.delete(tempDir);
    }

    private static TriplesBlock mkBlock(int capacity) throws IOException {
        Path file = Files.createTempFile("test", ".block");
        return new TriplesBlock(file, arena.scope(), capacity);
    }

    private static TriplesBlock mkOrdered(List<TestTriple> triples) throws IOException {
        TriplesBlock b = mkBlock(triples.size());
        for (TestTriple(long s, long p, long o) : triples)
                b.add(s, p, o);
        return b;
    }

    private static TriplesBlock mkReverse(List<TestTriple> triples) throws IOException {
        TriplesBlock b = mkBlock(triples.size());
        for (int i = triples.size()-1; i >= 0; --i) {
            TestTriple t = triples.get(i);
            b.add(t.s(), t.p(), t.o());
        }
        return b;
    }

    private static TriplesBlock mkShuffled(List<TestTriple> triples, long seed) throws IOException {
        TriplesBlock b = mkBlock(triples.size());
        ArrayList<TestTriple> shuffled = new ArrayList<>(triples);
        Collections.shuffle(shuffled, new Random(seed));
        for (TestTriple(long s, long p, long o) : shuffled)
                b.add(s, p, o);
        return b;
    }

    private static List<TestTriple> mkTriples(int... terms) {
        assertEquals(0, terms.length % 3, "partial triple in terms array");
        List<TestTriple> triples = new ArrayList<>();
        for (int i = 0; i < terms.length; i += 3)
            triples.add(new TestTriple(terms[i], terms[i+1], terms[i+2]));
        return triples;
    }

    private static List<TestTriple> readTriples(TriplesBlock b) {
        List<TestTriple> list = new ArrayList<>();
        var seg = b.seg;
        for (int i = 0, end = b.triples*24; i < end; i += 24) {
            list.add(new TestTriple(seg.get(JAVA_LONG, i),
                           seg.get(JAVA_LONG, i+8),
                           seg.get(JAVA_LONG, i+16)));
        }
        return list;
    }

    @Test void selfTestShuffle() {
        Assertions.assertEquals(new TestTriple(2, 1, 3), new TestTriple(1, 2, 3).spo2pso());
        Assertions.assertEquals(new TestTriple(3, 2, 1), new TestTriple(1, 2, 3).spo2pso().pso2ops());
    }

    static Stream<Arguments> test() throws IOException {
        List<List<TestTriple>> tripleLists = List.of(
                mkTriples(),
                mkTriples(1, 2, 3),
                mkTriples(1, 2, 3,     1, 2, 4),
                mkTriples(10, 1, 20,   11, 2, 21),
                mkTriples(10, 1, 20,   10, 1, 21,   11, 2, 21),
                mkTriples(10, 1, 20,   10, 1, 21,   11, 2, 21,   11, 2, 22),
                mkTriples(10, 1, 20,   10, 1, 21,   11, 2, 21,   11, 2, 22,
                          12, 1, 20,   12, 1, 21,   13, 2, 21,   13, 2, 22),
                mkTriples(10, 1, 20,   10, 2, 21,   10, 2, 22,   10, 2, 23,
                          11, 1, 20,   11, 2, 21,   11, 2, 22,   11, 2, 23,
                          12, 1, 20,   12, 2, 21,   12, 2, 22,   12, 2, 23,
                          13, 1, 20,   13, 2, 21,   13, 2, 22,   13, 2, 23)
        );
        List<Arguments> args = new ArrayList<>();
        for (List<TestTriple> triples : tripleLists) {
            args.add(arguments(triples, mkOrdered(triples)));
            args.add(arguments(triples, mkReverse(triples)));
            args.add(arguments(triples, mkShuffled(triples, 6199474)));
            args.add(arguments(triples, mkShuffled(triples, 9637695)));
            args.add(arguments(triples, mkShuffled(triples, 7171970)));
            args.add(arguments(triples, mkShuffled(triples, 287139 )));
            args.add(arguments(triples, mkShuffled(triples, 1690426)));
            args.add(arguments(triples, mkShuffled(triples, 1161375)));
        }
        return args.stream();
    }

    @ParameterizedTest @MethodSource("test") void testSort(List<TestTriple> triples, TriplesBlock block) {
        block.sort(0, block.tripleCount());
        assertEquals(triples, readTriples(block));
    }

    @ParameterizedTest @MethodSource("test") void testSortPSO(List<TestTriple> sorted,
                                                              TriplesBlock block) {
        var ex = readTriples(block);
        for (int i = 0, n = ex.size(); i < n; i++) ex.set(i, ex.get(i).spo2pso());
        block.spo2pso();
        assertEquals(ex, readTriples(block));

        Collections.sort(ex);
        block.sort(0, block.tripleCount());
        assertEquals(ex, readTriples(block));
    }

    @ParameterizedTest @MethodSource("test") void testSortOPS(List<TestTriple> sortedSPO,
                                                              TriplesBlock block) {
        var ex = readTriples(block);
        for (int i = 0, n = ex.size(); i < n; i++) ex.set(i, ex.get(i).spo2pso());
        block.spo2pso();
        assertEquals(ex, readTriples(block));

        for (int i = 0, n = ex.size(); i < n; i++) ex.set(i, ex.get(i).pso2ops());
        block.pso2ops();
        assertEquals(ex, readTriples(block));

        Collections.sort(ex);
        block.sort(0, block.tripleCount());
        assertEquals(ex, readTriples(block));
    }
}