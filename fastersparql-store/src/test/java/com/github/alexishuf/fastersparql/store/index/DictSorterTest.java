package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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

    @ParameterizedTest @ValueSource(ints = {0, 1, 2, 16, 32, 256})
    void test(int nStrings) throws IOException {
        List<SegmentRope> strings = new ArrayList<>(nStrings);
        for (int i = 0; i < nStrings; i++)
            strings.add(new ByteRope().append((long) nStrings - i));
        ByteRope tmp = new ByteRope();

        // build dict
        try (var sorter = new DictSorter(tempDir, 32)) {
            for (SegmentRope s : strings)
                sorter.storeCopy(tmp.clear().append(s));
            sorter.writeDict(tempDir.resolve("merged"));
        }

        try (var dict = new Dict(tempDir.resolve("merged"))) {
            assertEquals(strings.size(), dict.strings());
            // find all strings in merged dict
            for (SegmentRope s : strings) {
                long id = dict.find(s, tmp);
                assertTrue(id >= Dict.MIN_ID, "id="+id+", s="+s);
                assertEquals(s, dict.get(id), "id=" + id + ", s=" + s);
            }
            // find strings in reverse direction without giving a tmp
            for (int i = strings.size()-1; i >= 0; i--) {
                var s = strings.get(i);
                long id = dict.find(s, null);
                assertEquals(s, dict.get(id), "id=" + id + ", s=" + s);
            }
        }

        String[] filenames = tempDir.toFile().list();
        assertArrayEquals(new String[]{"merged"}, filenames);
    }
}