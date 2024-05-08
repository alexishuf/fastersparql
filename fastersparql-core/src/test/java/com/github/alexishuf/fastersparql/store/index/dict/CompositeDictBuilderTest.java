package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.PooledTwoSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CompositeDictBuilderTest {

    static Stream<Arguments> test() {
        List<Arguments> list = new ArrayList<>();
        for (Splitter.Mode mode : Splitter.Mode.values()) {
            for (boolean optimizeLocality : List.of(false, true))
                list.add(arguments(mode, optimizeLocality));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void test(Splitter.Mode mode, boolean optimizeLocality) throws IOException {
        Path temp = Files.createTempDirectory("fastersparql-temp");
        Path dest = Files.createTempDirectory("fastersparql-dest");
        List<FinalSegmentRope> ropes;
        try (var b = new CompositeDictBuilder(temp, dest, mode, optimizeLocality)) {
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
            ).map(FinalSegmentRope::asFinal).toList();
            for (var r : ropes)
                b.visit(r);
            var sndPass = b.nextPass();
            for (var r : ropes)
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
        Path sharedPath = dest.resolve("shared"), stringsPath = dest.resolve("strings");
        assertTrue(Files.isRegularFile(sharedPath));
        assertTrue(Files.isRegularFile(stringsPath));

        var splitters = Arrays.stream(Splitter.Mode.values())
                              .map(m -> Splitter.create(m).takeOwnership(this)).toList();
        //load dict
        Dict.AbstractLookup<?> lookup = null;
        try (var shared = Dict.loadStandalone(sharedPath);
             var dict = Dict.loadComposite(stringsPath, shared);
             var tsr = PooledTwoSegmentRope.ofEmpty()) {
            // check if dicts are valid
            shared.validate();
            dict.validate();

            // lookup all strings
            lookup = dict.polymorphicLookup().takeOwnership(this);
            for (PlainRope r : ropes) {
                long id = lookup.find(r);
                assertTrue(id >= Dict.MIN_ID, "r="+r);
                assertEquals(r, lookup.get(id));
                for (Splitter split : splitters) {
                    split.split(r);
                    tsr.wrapFirst ((SegmentRope)split.shared());
                    tsr.wrapSecond((SegmentRope)split.local());
                    if (split.sharedSide == Splitter.SharedSide.SUFFIX)
                        tsr.flipSegments();

                    id = lookup.find(tsr);
                    assertTrue(id >= Dict.MIN_ID);
                    assertEquals(r, lookup.get(id));
                }
            }

            // no extraneous strings
            assertEquals(ropes.size(), dict.strings());
        } finally {
            Owned.safeRecycle(lookup, this);
            for (var s : splitters) {
                Owned.safeRecycle(s, this);
            }
        }
    }

}