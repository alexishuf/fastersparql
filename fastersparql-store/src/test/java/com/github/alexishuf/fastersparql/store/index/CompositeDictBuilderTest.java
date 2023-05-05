package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompositeDictBuilderTest {

    @ParameterizedTest @EnumSource(Splitter.Mode.class)
    void test(Splitter.Mode mode) throws IOException {
        Path temp = Files.createTempDirectory("fastersparql-temp");
        Path dest = Files.createTempDirectory("fastersparql-dest");
        List<ByteRope> ropes;
        try (var b = new CompositeDictBuilder(temp, dest, mode)) {
            ropes = Stream.of(
                    "\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                    "\"bob\"",
                    "<http://example.org/>",
                    "<http://example.org/->",
                    "<http://example.org/Alice>",
                    "<http://example.org/TCGA-36-260x-g>",
                    "<http://example.org/TCGA-36-260x-g156>",
                    "<http://example.org/chebi:ex>",
                    "<http://example.org/places/123/name>"
            ).map(ByteRope::new).toList();
            for (ByteRope r : ropes)
                b.visit(r);
            var sndPass = b.nextPass();
            for (ByteRope r : ropes)
                sndPass.visit(r);
            sndPass.write();
        }
        // no temp files remain
        try (var files = Files.newDirectoryStream(temp)) {
            List<Path> actual = new ArrayList<>();
            files.forEach(actual::add);
            assertEquals(List.of(), actual);
        }
        // has shared and strings files
        assertTrue(Files.isRegularFile(dest.resolve("shared")));
        assertTrue(Files.isRegularFile(dest.resolve("strings")));

        //load dict
        try (var shared = new StandaloneDict(dest.resolve("shared"));
             var dict = new CompositeDict(dest.resolve("strings"), shared)) {
            // check if dicts are valid
            shared.validate();
            dict.validate();

            // lookup all strings
            var lookup = dict.lookup();
            var splitters = Arrays.stream(Splitter.Mode.values()).map(Splitter::new).toList();
            TwoSegmentRope tsr = new TwoSegmentRope();
            for (ByteRope r : ropes) {
                long id = lookup.find(r);
                assertTrue(id >= Dict.MIN_ID, "r="+r);
                assertEquals(r, lookup.get(id));
                for (Splitter split : splitters) {
                    split.split(r);
                    tsr.wrapFirst((SegmentRope) split.shared());
                    tsr.wrapSecond((SegmentRope) split.local());
                    if (split.sharedSide == Splitter.SharedSide.SUFFIX)
                        tsr.flipSegments();

                    id = lookup.find(tsr);
                    assertTrue(id >= Dict.MIN_ID);
                    assertEquals(r, lookup.get(id));
                }
            }

            // no extraneous strings
            assertEquals(ropes.size(), dict.strings());
        }
    }

}