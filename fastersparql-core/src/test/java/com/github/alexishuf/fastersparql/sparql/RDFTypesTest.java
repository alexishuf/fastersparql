package com.github.alexishuf.fastersparql.sparql;

import org.junit.jupiter.api.Test;

import static com.github.alexishuf.fastersparql.sparql.RDFTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class RDFTypesTest {
    @Test
    void testFromXsdLocal() {
        for (String full : XSD_IRIS) {
            String prefixed = "xsd:" + full.substring(XSD.length());
            String actual = fromXsdLocal(prefixed, 4, prefixed.length());
            assertEquals(full, actual);
            assertSame(full, actual);

            String bogus = ":"+ full.substring(XSD.length())+"x";
            assertEquals(full+"x", fromXsdLocal(bogus, 1, bogus.length()));
        }
    }

}