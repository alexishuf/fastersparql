package com.github.alexishuf.fastersparql.model.rope;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SharedRopesTest {
    static Stream<Arguments> testBuiltIns() {
        return Stream.of(
                SharedRopes.DT_duration,
                SharedRopes.DT_dateTime,
                SharedRopes.DT_time,
                SharedRopes.DT_date,
                SharedRopes.DT_gYearMonth,
                SharedRopes.DT_gYear,
                SharedRopes.DT_gMonthDay,
                SharedRopes.DT_gDay,
                SharedRopes.DT_gMonth,
                SharedRopes.DT_BOOLEAN,
                SharedRopes.DT_base64Binary,
                SharedRopes.DT_hexBinary,
                SharedRopes.DT_FLOAT,
                SharedRopes.DT_decimal,
                SharedRopes.DT_DOUBLE,
                SharedRopes.DT_anyURI,
                SharedRopes.DT_string,
                DT_integer,
                SharedRopes.DT_nonPositiveInteger,
                SharedRopes.DT_LONG,
                SharedRopes.DT_nonNegativeInteger,
                SharedRopes.DT_negativeInteger,
                SharedRopes.DT_INT,
                SharedRopes.DT_unsignedLong,
                SharedRopes.DT_positiveInteger,
                SharedRopes.DT_SHORT,
                SharedRopes.DT_unsignedInt,
                SharedRopes.DT_BYTE,
                SharedRopes.DT_unsignedShort,
                SharedRopes.DT_unsignedByte,
                SharedRopes.DT_normalizedString,
                SharedRopes.DT_token,
                SharedRopes.DT_language,
                SharedRopes.DT_langString,
                SharedRopes.DT_HTML,
                SharedRopes.DT_XMLLiteral,
                SharedRopes.DT_JSON,
                SharedRopes.DT_PlainLiteral,

                FinalSegmentRope.asFinal("<http://www.loc.gov/mads/rdf/v1#"),
                FinalSegmentRope.asFinal("<http://id.loc.gov/ontologies/bflc/"),
                FinalSegmentRope.asFinal("<http://xmlns.com/foaf/0.1/"),
                FinalSegmentRope.asFinal("<http://www.w3.org/2000/01/rdf-schema#"),
                FinalSegmentRope.asFinal("<http://yago-knowledge.org/resource/"),
                FinalSegmentRope.asFinal("<http://dbpedia.org/ontology/"),
                FinalSegmentRope.asFinal("<http://dbpedia.org/property/"),
                FinalSegmentRope.asFinal("<http://purl.org/dc/elements/1.1/"),
                FinalSegmentRope.asFinal("<http://example.org/"),
                FinalSegmentRope.asFinal("<http://www.w3.org/2002/07/owl#"),
                FinalSegmentRope.asFinal("<http://purl.org/goodrelations/v1#"),
                FinalSegmentRope.asFinal("<http://www.w3.org/2004/02/skos/core#"),
                FinalSegmentRope.asFinal("<http://data.ordnancesurvey.co.uk/ontology/spatialrelations/"),
                FinalSegmentRope.asFinal("<http://www.opengis.net/ont/geosparql#"),
                FinalSegmentRope.asFinal("<http://www.w3.org/ns/dcat#"),
                FinalSegmentRope.asFinal("<http://schema.org/"),
                FinalSegmentRope.asFinal("<http://www.w3.org/ns/org#"),
                FinalSegmentRope.asFinal("<http://purl.org/dc/terms/"),
                FinalSegmentRope.asFinal("<http://purl.org/linked-data/cube#"),
                FinalSegmentRope.asFinal("<http://id.loc.gov/ontologies/bibframe/"),
                FinalSegmentRope.asFinal("<http://www.w3.org/ns/prov#"),
                FinalSegmentRope.asFinal("<http://sindice.com/vocab/search#"),
                FinalSegmentRope.asFinal("<http://rdfs.org/sioc/ns#"),
                FinalSegmentRope.asFinal("<http://purl.org/xtypes/"),
                FinalSegmentRope.asFinal("<http://www.w3.org/ns/sparql-service-description#"),
                FinalSegmentRope.asFinal("<http://purl.org/net/ns/ontology-annot#"),
                FinalSegmentRope.asFinal("<http://rdfs.org/ns/void#"),
                FinalSegmentRope.asFinal("<http://purl.org/vocab/frbr/core#"),
                FinalSegmentRope.asFinal("<http://www.w3.org/ns/posix/stat#"),
                FinalSegmentRope.asFinal("<http://www.ontotext.com/"),
                FinalSegmentRope.asFinal("<http://www.w3.org/2006/vcard/ns#"),
                FinalSegmentRope.asFinal("<http://search.yahoo.com/searchmonkey/commerce/"),
                FinalSegmentRope.asFinal("<http://semanticscience.org/resource/"),
                FinalSegmentRope.asFinal("<http://purl.org/rss/1.0/"),
                FinalSegmentRope.asFinal("<http://purl.org/ontology/bibo/"),
                FinalSegmentRope.asFinal("<http://www.w3.org/ns/people#"),
                FinalSegmentRope.asFinal("<http://purl.obolibrary.org/obo/"),
                FinalSegmentRope.asFinal("<http://www.geonames.org/ontology#"),
                FinalSegmentRope.asFinal("<http://www.productontology.org/id/"),
                FinalSegmentRope.asFinal("<http://purl.org/NET/c4dm/event.owl#"),
                FinalSegmentRope.asFinal("<http://rdf.freebase.com/ns/"),
                FinalSegmentRope.asFinal("<http://www.wikidata.org/entity/"),
                FinalSegmentRope.asFinal("<http://purl.org/dc/dcmitype/"),
                FinalSegmentRope.asFinal("<http://purl.org/openorg/"),
                FinalSegmentRope.asFinal("<http://creativecommons.org/ns#"),
                FinalSegmentRope.asFinal("<http://purl.org/rss/1.0/modules/content/"),
                FinalSegmentRope.asFinal("<http://purl.org/gen/0.1#"),
                FinalSegmentRope.asFinal("<http://usefulinc.com/ns/doap#")

        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource void testBuiltIns(SegmentRope builtin) {
        try (var mutable = PooledMutableRope.get()) {
            if (builtin.get(0) == '<') {
                SegmentRope interned = SHARED_ROPES.internPrefix(builtin, 0, builtin.len);
                assertEquals(builtin, interned);
                builtin = interned;
                assertSame(builtin, SHARED_ROPES.internPrefix(builtin, 0, builtin.len));
                assertSame(builtin, SHARED_ROPES.internPrefix(mutable.append(builtin), 0, builtin.len));
                assertSame(builtin, SHARED_ROPES.internPrefix(builtin.toString()));
            } else {
                assertEquals('"', builtin.get(0));
                assertSame(builtin, SHARED_ROPES.internDatatype(builtin, 0, builtin.len));
                assertSame(builtin, SHARED_ROPES.internDatatype(mutable.append(builtin), 0, builtin.len));
                assertSame(builtin, SHARED_ROPES.internDatatype(builtin.toString()));
            }
        }

        TwoSegmentRope tsr = new TwoSegmentRope();
        List<Long> mids = List.of(builtin.offset + builtin.len / 2,
                builtin.offset + builtin.skipUntilLast(0, builtin.len, (byte)'/', (byte)'#'),
                builtin.offset + builtin.skipUntilLast(0, builtin.len, (byte)'"'));
        for (long mid : mids) {
            tsr.wrapFirst(builtin.segment, builtin.utf8, builtin.offset, (int)(mid-builtin.offset));
            tsr.wrapSecond(builtin.segment, builtin.utf8, mid, (int)(builtin.offset+builtin.len-mid));
            if (builtin.get(0) == '<')
                assertSame(builtin, SHARED_ROPES.internPrefix(tsr, 0, tsr.len));
            else
                assertSame(builtin, SHARED_ROPES.internDatatype(tsr, 0, tsr.len));
        }
    }

    static Stream<Arguments> internPrefix() {
        return Stream.of(
                arguments("<http://example.org/Alice>", "<http://example.org/"),
                arguments("<http://example.org/ns#Alice>", "<http://example.org/ns#"),
                arguments("<http://example.org/>", "<http://example.org/"),
                arguments("<looooooooooooooooog/rel>", "<looooooooooooooooog/"),
                arguments("<rel>", ""),
                arguments("</rel>", ""),
                arguments("<#loooooooooooooooooooooooooooooooooooooog-rel>", "")
        );
    }

    @ParameterizedTest @MethodSource void internPrefix(String iri, String prefix) {
        try (var r = PooledMutableRope.get().append("([" + iri + "])")) {
            TwoSegmentRope tsr0 = new TwoSegmentRope(), tsr1 = new TwoSegmentRope();
            int half = r.len / 2;
            tsr0.wrapFirst(r.segment, r.utf8, r.offset, half);
            tsr0.wrapSecond(r.segment, r.utf8, r.offset + half, r.len - half);
            half = r.skipUntilLast(0, r.len, (byte)'/', (byte)'#');
            tsr1.wrapFirst(r.segment, r.utf8, r.offset, half);
            tsr1.wrapSecond(r.segment, r.utf8, r.offset + half, r.len - half);

            List<PlainRope> ropes = List.of(r, tsr0, tsr1);
            for (int i = 0; i < ropes.size(); i++) {
                SegmentRope interned;
                if (ropes.get(i) instanceof SegmentRope s) {
                    interned = SHARED_ROPES.internPrefixOf(s, 2, r.len - 2);
                    assertEquals(String.valueOf(prefix), String.valueOf(interned), "i=" + i);
                }
                interned = SHARED_ROPES.internPrefixOf(ropes.get(i), 2, r.len - 2);
                assertEquals(String.valueOf(prefix), String.valueOf(interned), "i=" + i);
            }
        }
    }

    static Stream<Arguments> internSuffix() {
        return Stream.of(
                arguments("\"bob\"", ""),
                arguments("\"alice\"@en", ""),
                arguments("\"alice\"@en-US", ""),
                arguments("\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
                arguments("\"23\"\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
                arguments("\"23\"/#\"\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"^^<http://www.w3.org/2001/XMLSchema#integer>")
        );
    }

    @ParameterizedTest @MethodSource void internSuffix(String lit, String suffix) {
        try (var r = PooledMutableRope.get().append("([" + lit + "])")) {
            TwoSegmentRope tsr0 = new TwoSegmentRope(), tsr1 = new TwoSegmentRope();
            tsr0.wrapFirst(r.segment, r.utf8, r.offset, r.len / 2);
            tsr0.wrapSecond(r.segment, r.utf8, r.offset + r.len / 2, r.len);
            int mid = r.skipUntilLast(0, r.len, (byte)'"');
            tsr1.wrapFirst(r.segment, r.utf8, r.offset, mid);
            tsr1.wrapSecond(r.segment, r.utf8, r.offset + mid, r.len - mid);

            List<PlainRope> ropes = List.of(r, tsr0, tsr1);
            for (int i = 0; i < ropes.size(); i++) {
                SegmentRope interned;
                if (ropes.get(i) instanceof SegmentRope s) {
                    interned = SHARED_ROPES.internDatatypeOf(s, 2, r.len - 2);
                    assertEquals(String.valueOf(suffix), String.valueOf(interned), "i=" + i);
                }
                interned = SHARED_ROPES.internDatatypeOf(ropes.get(i), 2, r.len - 2);
                assertEquals(String.valueOf(suffix), String.valueOf(interned), "i=" + i);
            }
        }
    }


    @Test void testOverflowBucket() {
        try (var r = PooledMutableRope.get().append(DT_integer)) {
            byte original = r.get(r.len - 2);
            do {
                SegmentRope interned = SHARED_ROPES.internDatatype(r, 0, r.len);
                assertEquals(r, interned);
                assertNotSame(r, interned);
                r.u8()[r.len - 2] += 2;
            } while (r.get(r.len - 2) != original);
            assertSame(DT_integer, SHARED_ROPES.internDatatype(FinalSegmentRope.asFinal(DT_integer), 0, r.len));
            assertSame(DT_integer, SHARED_ROPES.internDatatype(FinalSegmentRope.asFinal(DT_integer.toString()), 0, r.len));
            assertSame(DT_integer, SHARED_ROPES.internDatatype(FinalSegmentRope.asFinal(DT_integer.toString().getBytes(UTF_8)), 0, r.len));
        }
    }
}