package com.github.alexishuf.fastersparql.model.rope;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
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

                new ByteRope("<http://www.loc.gov/mads/rdf/v1#"),
                new ByteRope("<http://id.loc.gov/ontologies/bflc/"),
                new ByteRope("<http://xmlns.com/foaf/0.1/"),
                new ByteRope("<http://www.w3.org/2000/01/rdf-schema#"),
                new ByteRope("<http://yago-knowledge.org/resource/"),
                new ByteRope("<http://dbpedia.org/ontology/"),
                new ByteRope("<http://dbpedia.org/property/"),
                new ByteRope("<http://purl.org/dc/elements/1.1/"),
                new ByteRope("<http://example.org/"),
                new ByteRope("<http://www.w3.org/2002/07/owl#"),
                new ByteRope("<http://purl.org/goodrelations/v1#"),
                new ByteRope("<http://www.w3.org/2004/02/skos/core#"),
                new ByteRope("<http://data.ordnancesurvey.co.uk/ontology/spatialrelations/"),
                new ByteRope("<http://www.opengis.net/ont/geosparql#"),
                new ByteRope("<http://www.w3.org/ns/dcat#"),
                new ByteRope("<http://schema.org/"),
                new ByteRope("<http://www.w3.org/ns/org#"),
                new ByteRope("<http://purl.org/dc/terms/"),
                new ByteRope("<http://purl.org/linked-data/cube#"),
                new ByteRope("<http://id.loc.gov/ontologies/bibframe/"),
                new ByteRope("<http://www.w3.org/ns/prov#"),
                new ByteRope("<http://sindice.com/vocab/search#"),
                new ByteRope("<http://rdfs.org/sioc/ns#"),
                new ByteRope("<http://purl.org/xtypes/"),
                new ByteRope("<http://www.w3.org/ns/sparql-service-description#"),
                new ByteRope("<http://purl.org/net/ns/ontology-annot#"),
                new ByteRope("<http://rdfs.org/ns/void#"),
                new ByteRope("<http://purl.org/vocab/frbr/core#"),
                new ByteRope("<http://www.w3.org/ns/posix/stat#"),
                new ByteRope("<http://www.ontotext.com/"),
                new ByteRope("<http://www.w3.org/2006/vcard/ns#"),
                new ByteRope("<http://search.yahoo.com/searchmonkey/commerce/"),
                new ByteRope("<http://semanticscience.org/resource/"),
                new ByteRope("<http://purl.org/rss/1.0/"),
                new ByteRope("<http://purl.org/ontology/bibo/"),
                new ByteRope("<http://www.w3.org/ns/people#"),
                new ByteRope("<http://purl.obolibrary.org/obo/"),
                new ByteRope("<http://www.geonames.org/ontology#"),
                new ByteRope("<http://www.productontology.org/id/"),
                new ByteRope("<http://purl.org/NET/c4dm/event.owl#"),
                new ByteRope("<http://rdf.freebase.com/ns/"),
                new ByteRope("<http://www.wikidata.org/entity/"),
                new ByteRope("<http://purl.org/dc/dcmitype/"),
                new ByteRope("<http://purl.org/openorg/"),
                new ByteRope("<http://creativecommons.org/ns#"),
                new ByteRope("<http://purl.org/rss/1.0/modules/content/"),
                new ByteRope("<http://purl.org/gen/0.1#"),
                new ByteRope("<http://usefulinc.com/ns/doap#")

        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource void testBuiltIns(SegmentRope builtin) {
        if (builtin.get(0) == '<') {
            SegmentRope interned = SHARED_ROPES.internPrefix(builtin, 0, builtin.len);
            assertEquals(builtin, interned);
            builtin = interned;
            assertSame(builtin, SHARED_ROPES.internPrefix(builtin, 0, builtin.len));
            assertSame(builtin, SHARED_ROPES.internPrefix(new ByteRope().append(builtin), 0, builtin.len));
            assertSame(builtin, SHARED_ROPES.internPrefix(builtin.toString()));
        } else {
            assertEquals('"', builtin.get(0));
            assertSame(builtin, SHARED_ROPES.internDatatype(builtin, 0, builtin.len));
            assertSame(builtin, SHARED_ROPES.internDatatype(new ByteRope().append(builtin), 0, builtin.len));
            assertSame(builtin, SHARED_ROPES.internDatatype(builtin.toString()));
        }

        TwoSegmentRope tsr = new TwoSegmentRope();
        List<Long> mids = List.of(builtin.offset + builtin.len / 2,
                builtin.offset + builtin.skipUntilLast(0, builtin.len, '/', '#'),
                builtin.offset + builtin.skipUntilLast(0, builtin.len, '"'));
        for (long mid : mids) {
            tsr.wrapFirst(builtin.segment, builtin.utf8, builtin.offset, (int) mid);
            tsr.wrapSecond(builtin.segment, builtin.utf8, mid, builtin.len-(int)mid);
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
        ByteRope r = new ByteRope("([" + iri + "])");
        TwoSegmentRope tsr0 = new TwoSegmentRope(), tsr1 = new TwoSegmentRope();
        int half = r.len/2;
        tsr0.wrapFirst(r.segment, r.utf8, r.offset, half);
        tsr0.wrapSecond(r.segment, r.utf8, r.offset+half, r.len-half);
        half = r.skipUntilLast(0, r.len, '/', '#');
        tsr1.wrapFirst(r.segment, r.utf8, r.offset, half);
        tsr1.wrapSecond(r.segment, r.utf8, r.offset+half, r.len-half);

        List<PlainRope> ropes = List.of(r, tsr0, tsr1);
        for (int i = 0; i < ropes.size(); i++) {
            SegmentRope interned;
            if (ropes.get(i) instanceof SegmentRope s) {
                interned = SHARED_ROPES.internPrefixOf(s, 2, r.len-2);
                assertEquals(String.valueOf(prefix), String.valueOf(interned), "i="+i);
            }
            interned = SHARED_ROPES.internPrefixOf(ropes.get(i), 2, r.len-2);
            assertEquals(String.valueOf(prefix), String.valueOf(interned), "i="+i);
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
        ByteRope r = new ByteRope("([" + lit + "])");
        TwoSegmentRope tsr0 = new TwoSegmentRope(), tsr1 = new TwoSegmentRope();
        tsr0.wrapFirst(r.segment, r.utf8, r.offset, r.len/2);
        tsr0.wrapSecond(r.segment, r.utf8, r.offset+r.len/2, r.len);
        int mid = r.skipUntilLast(0, r.len, '"');
        tsr1.wrapFirst(r.segment, r.utf8, r.offset, mid);
        tsr1.wrapSecond(r.segment, r.utf8, r.offset+mid, r.len-mid);

        List<PlainRope> ropes = List.of(r, tsr0, tsr1);
        for (int i = 0; i < ropes.size(); i++) {
            SegmentRope interned;
            if (ropes.get(i) instanceof SegmentRope s) {
                interned = SHARED_ROPES.internDatatypeOf(s, 2, r.len-2);
                assertEquals(String.valueOf(suffix), String.valueOf(interned), "i=" + i);
            }
            interned = SHARED_ROPES.internDatatypeOf(ropes.get(i), 2, r.len-2);
            assertEquals(String.valueOf(suffix), String.valueOf(interned), "i="+i);
        }
    }


    @Test void testOverflowBucket() {
        ByteRope r = new ByteRope().append(DT_integer);
        byte original = r.get(r.len - 2);
        do {
            SegmentRope interned = SHARED_ROPES.internDatatype(r, 0, r.len);
            assertEquals(r, interned);
            assertNotSame(r, interned);
            r.u8()[r.len-2] += 2;
        } while (r.get(r.len-2) != original);
        assertSame(DT_integer, SHARED_ROPES.internDatatype(new ByteRope(DT_integer), 0, r.len));
    }
}