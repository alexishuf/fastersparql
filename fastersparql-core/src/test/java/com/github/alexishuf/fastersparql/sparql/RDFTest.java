package com.github.alexishuf.fastersparql.sparql;

import org.junit.jupiter.api.Test;

import static com.github.alexishuf.fastersparql.sparql.RDF.NS;
import static com.github.alexishuf.fastersparql.sparql.RDF.fromRdfLocal;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class RDFTest {
    @Test
    void testFromRdfLocal() {
        for (String iri : RDF.RDF_IRIS) {
            String local = iri.substring(RDF.NS.length());
            int len = local.length();
            assertSame(iri, fromRdfLocal(local, 0, len));
            assertSame(iri, fromRdfLocal("rdf:"+local, 4, 4+len));
            assertSame(iri, fromRdfLocal(local+",", 0, len));
            assertSame(iri, fromRdfLocal("rdf:"+local+",", 4, 4+len));
        }
    }

    @Test
    void testFromRdfLocalBogus() {
        assertEquals(NS+"bogus", fromRdfLocal("bogus", 0, 5));
        assertEquals(NS+"bogus", fromRdfLocal("bogus,", 0, 5));
        assertEquals(NS+"bogus", fromRdfLocal("rdf:bogus", 4, 9));
        assertEquals(NS+"bogus", fromRdfLocal("rdf:bogus,", 4, 9));
    }

}