package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class DictSorterTest {
    private static Path tempDir;

    @BeforeAll static void beforeAll() throws IOException {
        tempDir = Files.createTempDirectory("dict-sorter");
    }

    @AfterAll static void afterAll() {
        for (File f : List.of(tempDir.resolve("merged").toFile(), tempDir.toFile())) {
            if (f.exists() && !f.delete())
                fail("Could not delete "+f);
        }
    }

    static Stream<Arguments> test() {
        List<Integer> sizes = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                                      15, 16, 17, 32, 255, 256, 257, 1024);
        List<Arguments> list = new ArrayList<>();
        for (int size : sizes) {
            for (boolean localityOptimized : List.of(false, true))
                list.add(arguments(size, localityOptimized));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void test(int nStrings, boolean localityOptimized) throws IOException {
        List<SegmentRope> strings = new ArrayList<>(nStrings);
        for (int i = 0; i < nStrings; i++)
            strings.add(new ByteRope().append((long) nStrings - i));

        // build dict
        Path mergedPath = tempDir.resolve("merged");
        try (var sorter = new DictSorter(tempDir, false, localityOptimized, 32)) {
            ByteRope tmp = new ByteRope();
            int i = 0;
            for (SegmentRope s : strings) {
                sorter.copy(tmp.clear().append(s));
                if ((i++ &1) == 0)
                    sorter.copy(tmp.clear().append(s));
            }
            for (SegmentRope s : strings)
                sorter.copy(tmp.clear().append(s));
            sorter.writeDict(mergedPath);
        }

        try (Dict dict = Dict.loadStandalone(mergedPath)) {
            Dict.AbstractLookup lookup;
            if (dict instanceof LocalityStandaloneDict d)
                lookup = d.lookup();
            else
                lookup = ((SortedStandaloneDict)dict).lookup();
            assertEquals(strings.size(), dict.strings());
            // find all strings in merged dict
            for (SegmentRope s : strings) {
                long id = lookup.find(s);
                assertTrue(id >= Dict.MIN_ID, "id="+id+", s="+s);
                assertEquals(s, lookup.get(id), "id=" + id + ", s=" + s);
            }
            // find strings in reverse direction without giving a tmp
            for (int i = strings.size()-1; i >= 0; i--) {
                SegmentRope s = strings.get(i);
                long id = lookup.find(s);
                assertEquals(s, lookup.get(id), "id=" + id + ", s=" + s);
            }

            //test dump()
            if (dict.isSorted()) {
                var exBuilder = new StringBuilder();
                strings.stream().sorted().forEach(r -> exBuilder.append(r).append('\n'));
                exBuilder.setLength(Math.max(0, exBuilder.length() - 1));
                assertEquals(exBuilder.toString(), dict.dump());
            }
        }

        String[] filenames = tempDir.toFile().list();
        assertArrayEquals(new String[]{"merged"}, filenames);
    }
}