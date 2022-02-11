package com.github.alexishuf.fastersparql.jena;

import com.github.alexishuf.fastersparql.operators.expressions.RDFValues;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.jena.graph.NodeFactory.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class JenaUtilsTest {
    static Stream<Arguments> testFromNt() {
        return Stream.of(
                arguments("\"false\"^^<"+RDFValues.BOOLEAN+">",
                          createLiteral("false", XSDDatatype.XSDboolean)),
                arguments("false", createLiteral("false", XSDDatatype.XSDboolean)),
                arguments("true", createLiteral("true", XSDDatatype.XSDboolean)),
                arguments("\"bob\"", createLiteral("bob")),
                arguments("\"bob\"@en", createLiteral("bob", "en")),
                arguments("\"bob\"@en-US", createLiteral("bob", "en-US")),
                arguments("\"bob\"@en_US", createLiteral("bob", "en-US")),
                arguments("\"bob\"^^<"+RDFValues.string+">",
                          createLiteral("bob", XSDDatatype.XSDstring)),
                arguments("'bob'@en", createLiteral("bob", "en")),
                arguments("\"<\\\"^\"", createLiteral("<\"^")),
                arguments("23", createLiteral("23", XSDDatatype.XSDinteger)),
                arguments("23.0", createLiteral("23.0", XSDDatatype.XSDdecimal)),
                arguments("1E9", createLiteral("1E9", XSDDatatype.XSDdouble)),
                arguments("_:blank", createBlankNode("blank")),
                arguments("<a>", createURI("a")),
                arguments("<http://example.org/Alice?x=2>",
                          createURI("http://example.org/Alice?x=2"))
        );
    }

    @ParameterizedTest @MethodSource
    void testFromNt(String nt, Node expected) {
        assertEquals(expected, JenaUtils.fromNT(nt));
    }
}