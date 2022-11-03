package com.github.alexishuf.fastersparql.sparql;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.sparql.RDFTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RDFTypesTest {
    public static Stream<Arguments> testIsXsd() {
        List<Arguments> list = new ArrayList<>(List.of(
                arguments(false, "", string),
                arguments(false, "ex:String", string),
                arguments(false, ":string", string),
                arguments(false, "http://example.org/ns#string", string),
                arguments(false, "<http://example.org/ns#string>", string)
        ));
        for (String iri : List.of(string, RDFTypes.integer, RDFTypes.INT, RDFTypes.LONG, RDFTypes.FLOAT, RDFTypes.DOUBLE, RDFTypes.integer, RDFTypes.decimal, RDFTypes.BOOLEAN)) {
            list.add(arguments(true, iri, iri));
            list.add(arguments(true, "<"+iri+">", iri));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void testIsXsd(boolean expected, String iri, String xsdIri) {
        assertEquals(expected, RDFTypes.isXsd(iri, xsdIri));
    }

    static Stream<Arguments> testExpandPrefixed() {
        List<Arguments> list = new ArrayList<>();
        for (String iri : XSD_IRIS)
            list.add(arguments("xsd:"+iri.substring(XSD.length()), iri));
        for (String iri : RDF_IRIS)
            list.add(arguments("rdf:"+iri.substring(RDF.length()), iri));
        list.addAll(List.of(
                arguments("bob", null),
                arguments("xs:", null),
                arguments("xsd", null),
                arguments("xsdinteger", null),
                arguments("xsd:", null)
        ));
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void testExpandPrefixed(String mid, String expected) {
        for (String prefix : List.of("", ":", "xsd:")) {
            for (String suffix : List.of("", ".", "-", "_")) {
                String in = prefix + mid + suffix;
                assertSame(expected, expandPrefixed(in, prefix.length(), in.length()),
                           "prefix=\""+prefix+"\", suffix=\""+suffix+"\"");
            }
        }
    }
}