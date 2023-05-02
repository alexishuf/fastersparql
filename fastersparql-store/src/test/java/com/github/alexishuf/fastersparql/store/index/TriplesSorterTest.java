package com.github.alexishuf.fastersparql.store.index;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

class TriplesSorterTest {
    private static Path tempDir, destDir;

    @BeforeAll static void beforeAll() throws IOException {
        tempDir = Files.createTempDirectory("fastersparql-temp");
        destDir = Files.createTempDirectory("fastersparql-dest");
    }

    @AfterAll static void afterAll() throws IOException {
        for (Path dir : List.of(tempDir, destDir)) {
            try (var stream = Files.newDirectoryStream(dir)) {
                for (Path p : stream) Files.deleteIfExists(p);
            }
            Files.deleteIfExists(dir);
        }
    }

    static Stream<Arguments> test() {
        return Stream.of(
                List.of(),
                List.of(new TestTriple(1, 2, 3)),
                List.of(new TestTriple(10, 1, 20), new TestTriple(11, 2, 21)),
                List.of(new TestTriple(10, 1, 20), new TestTriple(10, 2, 21)),
                List.of(new TestTriple(10, 1, 20), new TestTriple(11, 1, 21)),
                List.of(new TestTriple(10, 1, 20), new TestTriple(11, 2, 20)),
                List.of(new TestTriple(10, 1, 20), new TestTriple(11, 1, 20)),
                List.of(new TestTriple(10, 1, 20), new TestTriple(10, 1, 21)),

                List.of(new TestTriple(10, 1, 20), new TestTriple(10, 1, 21),
                        new TestTriple(10, 1, 22), new TestTriple(10, 1, 23),
                        new TestTriple(10, 1, 24), new TestTriple(10, 1, 25)),
                List.of(new TestTriple(10, 1, 20), new TestTriple(11, 1, 20),
                        new TestTriple(11, 1, 21), new TestTriple(11, 1, 22),
                        new TestTriple(11, 1, 23), new TestTriple(11, 1, 24),
                        new TestTriple(11, 1, 25)),
                List.of(new TestTriple(10, 1, 20), new TestTriple(11, 1, 20),
                        new TestTriple(11, 1, 21), new TestTriple(11, 1, 22),
                        new TestTriple(11, 1, 23), new TestTriple(11, 1, 24),
                        new TestTriple(11, 1, 25), new TestTriple(12, 1, 25))
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    public void test(List<TestTriple> triples) throws Exception {
        try (var tester = IterationTester.create(triples)) {
            tester.testIteration();
        }
    }


}