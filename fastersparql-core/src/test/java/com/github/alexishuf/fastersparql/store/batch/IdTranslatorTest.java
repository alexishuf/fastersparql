package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.store.StoreSparqlClient;
import com.github.alexishuf.fastersparql.store.index.dict.CompositeDictBuilder;
import com.github.alexishuf.fastersparql.store.index.dict.Dict;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityCompositeDict;
import com.github.alexishuf.fastersparql.store.index.triples.TriplesSorter;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.store.index.dict.Splitter.Mode.LAST;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.*;

class IdTranslatorTest {

    private static final List<FinalSegmentRope> TERMS = Stream.of(
            "<http://example.org/ontology/predicate>",
            "<http://example.org/ontology/object>",
            "<http://example.org/1>",
            "<http://example.org/2>",
            "<http://example.org/3>",
            "_:b0",
            "\"23\"^^<http://www.w3.org/2001/XMLSchema#>",
            "\"alice\"@en",
            "\"bob\""
    ).map(FinalSegmentRope::asFinal).toList();

    private static Path testDict;

    @BeforeAll
    static void beforeAll() throws IOException {
        Path tempDir = Files.createTempDirectory("fastersparql");
        tempDir.toFile().deleteOnExit();
        try (var b = new CompositeDictBuilder(tempDir, tempDir, LAST, true)) {
            for (var t : TERMS)
                b.visit(t);
            var secondPass = b.nextPass();
            for (var t : TERMS)
                secondPass.visit(t);
            secondPass.write();
        }
        try (var dict = (LocalityCompositeDict)Dict.load(tempDir.resolve("strings"));
             var sorter = new TriplesSorter(tempDir);
             var lookupG = new Guard<LocalityCompositeDict.Lookup>(IdTranslatorTest.class)) {
            var lookup = lookupG.set(dict.lookup());
            long p = lookup.find(TERMS.getFirst());
            long o = lookup.find(TERMS.get(1));
            for (FinalSegmentRope term : TERMS)
                sorter.addTriple(lookup.find(term), p, o);
            sorter.write(tempDir);
        }
        testDict = tempDir;
    }

    @AfterAll static void afterAll() throws IOException {
        try (var stream = Files.newDirectoryStream(testDict)) {
            for (Path p : stream)
                Files.deleteIfExists(p);
        }
        Files.deleteIfExists(testDict);
    }

    @Test public void test() throws IOException {
        int dictId;
        try (var client = new StoreSparqlClient(SparqlEndpoint.parse("file://"+testDict))) {
            LocalityCompositeDict dict = IdTranslator.dict(dictId = client.dictId());
            for (int i = 0; i < 8; ++i) {
                var lookup = dict.lookup().takeOwnership(this);
                try {
                    for (var t : TERMS) {
                        long id = lookup.find(t);
                        assertTrue(id >= Dict.MIN_ID);
                        //noinspection AssertBetweenInconvertibleTypes
                        assertEquals(t, lookup.get(id));

                        long sourced = IdTranslator.source(id, dictId);
                        assertEquals(id, IdTranslator.unsource(sourced));
                        assertEquals(dictId, IdTranslator.dictId(sourced));
                        assertSame(dict, IdTranslator.dict(dictId));
                    }
                } finally {
                    lookup.recycle(this);
                }
            }
        }
        assertNull(IdTranslator.dict(dictId));
    }

    @RepeatedTest(10)
    void testConcurrent() throws Exception {
        int threads = 2*Runtime.getRuntime().availableProcessors();
        try (var set = new TestTaskSet(getClass().getSimpleName(), newFixedThreadPool(threads))) {
            set.repeat(IdTranslator.MAX_DICT, () -> { test(); return null; });
        }
    }


}