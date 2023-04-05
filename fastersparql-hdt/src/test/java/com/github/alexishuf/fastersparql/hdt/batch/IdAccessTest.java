package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.dictionary.impl.FourSectionDictionary;
import org.rdfhdt.hdt.dictionary.impl.section.PFCDictionarySection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.options.HDTSpecification;

import java.util.List;

import static com.github.alexishuf.fastersparql.client.util.TestTaskSet.virtualRepeatAndWait;
import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.rdfhdt.hdt.enums.TripleComponentRole.*;

class IdAccessTest {

    public static final String ALICE_S = "http://example.org/Alice";
    public static final String BOB_S = "http://example.org/Bob";
    public static final String CHARLIE_S = "\"charlie\"";
    public static final String KNOWS_S = "http://xmlns.com/foaf/0.1/knows";

    public static final Term ALICE_T = Term.valueOf("<http://example.org/Alice>");
    public static final Term BOB_T = Term.valueOf("<http://example.org/Bob>");
    public static final Term CHARLIE_T = Term.valueOf("\"charlie\"");
    public static final Term KNOWS_T = Term.valueOf("<http://xmlns.com/foaf/0.1/knows>");

    public static long Alice(Dictionary dictionary) {
        return dictionary.stringToId(ALICE_S, OBJECT);
    }
    public static long Bob(Dictionary dictionary) {
        return dictionary.stringToId(BOB_S, SUBJECT);
    }
    public static long charlie(Dictionary dictionary) {
        return dictionary.stringToId(CHARLIE_S, OBJECT);
    }
    public static long knows(Dictionary dictionary) {
        return dictionary.stringToId(KNOWS_S, PREDICATE);
    }

    public static long Alice(int dictId) {
        Dictionary d = IdAccess.dict(dictId);
        long plain = d.stringToId(ALICE_S, OBJECT);
        return encode(plain, dictId, OBJECT);
    }
    public static long Bob(int dictId) {
        Dictionary d = IdAccess.dict(dictId);
        long plain = d.stringToId(BOB_S, SUBJECT);
        return encode(plain, dictId, SUBJECT);
    }
    public static long charlie(int dictId) {
        Dictionary d = IdAccess.dict(dictId);
        long plain = d.stringToId(CHARLIE_S, OBJECT);
        return encode(plain, dictId, OBJECT);
    }
    public static long knows(int dictId) {
        Dictionary d = IdAccess.dict(dictId);
        long plain = d.stringToId(KNOWS_S, PREDICATE);
        return encode(plain, dictId, PREDICATE);
    }

    public static Dictionary dummyDict() {
        var dict = new FourSectionDictionary(new HDTSpecification());
        ((PFCDictionarySection)dict.getShared())
                .load(List.of(ALICE_S).iterator(), 1, null);
        ((PFCDictionarySection)dict.getSubjects())
                .load(List.of(BOB_S).iterator(), 1, null);
        ((PFCDictionarySection)dict.getPredicates())
                .load(List.of(KNOWS_S).iterator(), 1, null);
        ((PFCDictionarySection)dict.getObjects())
                .load(List.of(CHARLIE_S).iterator(), 1, null);
        return dict;
    }

    @RepeatedTest(2) public void testDictLifecycle() {
        Dictionary d1 = dummyDict();
        Dictionary d2 = dummyDict();
        Dictionary d3 = dummyDict();

        int id1 = IdAccess.register(d1);
        assertTrue(id1 > 0);
        assertSame(d1, IdAccess.dict(id1));

        int id2 = IdAccess.register(d2);
        assertNotEquals(id1, id2);
        assertSame(d1, IdAccess.dict(id1));
        assertSame(d2, IdAccess.dict(id2));

        IdAccess.release(id1);
        IdAccess.release(id2);
        assertThrows(IdAccess.NoDictException.class, () -> IdAccess.dict(id1));
        assertThrows(IdAccess.NoDictException.class, () -> IdAccess.dict(id2));

        int id3 = IdAccess.register(d3);
        assertNotEquals(id1, id3);
        assertNotEquals(id2, id3);
        assertSame(d3, IdAccess.dict(id3));
    }

    @Test public void testDictLifecycleConcurrent() throws Exception {
        virtualRepeatAndWait("testDictLifecycle", 100,
                             this::testDictLifecycle);
    }

    @RepeatedTest(2) public void testEncode() {
        Dictionary d = dummyDict();
        int dId = IdAccess.register(d);
        try {
            for (long sourced : List.of(Alice(dId), Bob(dId), charlie(dId), knows(dId))) {
                long plain = IdAccess.plain(sourced);
                int dictId = IdAccess.dictId(sourced);
                TripleComponentRole role = IdAccess.role(sourced);
                assertEquals(sourced, encode(plain, dictId, role));
            }
            assertEquals(  ALICE_S, IdAccess.toString(Alice(dId)));
            assertEquals(    BOB_S, IdAccess.toString(Bob(dId)));
            assertEquals(CHARLIE_S, IdAccess.toString(charlie(dId)));
            assertEquals(  KNOWS_S, IdAccess.toString(knows(dId)));
        } finally {
            IdAccess.release(dId);
        }
    }

    @Test public void testEncodeConcurrent() throws Exception {
        virtualRepeatAndWait("testEncode", 100, this::testEncode);
    }

    @Test public void testEncodeFromString() {
        var d = dummyDict();
        int dId = IdAccess.register(d);
        try {
            long alice = encode(dId, ALICE_T);
            long bob = encode(dId, BOB_T);
            long charlie = encode(dId, CHARLIE_T);
            long knows = encode(dId, KNOWS_T);
            assertEquals(ALICE_S, IdAccess.toString(alice));
            assertEquals(BOB_S, IdAccess.toString(bob));
            assertEquals(CHARLIE_S, IdAccess.toString(charlie));
            assertEquals(KNOWS_S, IdAccess.toString(knows));
            assertEquals(Alice(dId)&~ROLE_MASK, alice&~ROLE_MASK);
            assertEquals(Bob(dId), bob);
            assertEquals(charlie(dId), charlie);
            assertEquals(knows(dId), knows);

            assertEquals(Alice(dId), IdAccess.encode(dId, d, OBJECT, ALICE_T));
            assertEquals(Bob(dId), IdAccess.encode(dId, d, SUBJECT, BOB_T));
            assertEquals(charlie(dId), IdAccess.encode(dId, d, OBJECT, CHARLIE_T));
            assertEquals(knows(dId), IdAccess.encode(dId, d, PREDICATE, KNOWS_T));

            assertEquals(NOT_FOUND, IdAccess.encode(dId, d, PREDICATE, ALICE_T));
            assertEquals(NOT_FOUND, IdAccess.encode(dId, d, OBJECT, KNOWS_T));
            assertEquals(NOT_FOUND, IdAccess.encode(dId, d, SUBJECT, CHARLIE_T));

            assertEquals(0, IdAccess.encode(dId, d, SUBJECT, ""));
            assertEquals(0, IdAccess.encode(dId, d, PREDICATE, "?x"));
            assertEquals(0, IdAccess.encode(dId, d, OBJECT, Term.valueOf("?y")));
            assertEquals(0, IdAccess.encode(dId, d, ""));
            assertEquals(0, IdAccess.encode(dId, d, "?x"));
            assertEquals(0, IdAccess.encode(dId, d, Term.valueOf("?y")));
        } finally {
            IdAccess.release(dId);
        }
    }

    @Test public void testEncodeFromStringConcurrent() throws Exception {
        virtualRepeatAndWait("testEncodeFromString", 100, this::testEncodeFromString);
    }


    @Test public void testToRole() {
        var d = dummyDict();
        int dId = IdAccess.register(d);
        try {
            assertEquals(0, toRole(SUBJECT,   0));
            assertEquals(0, toRole(PREDICATE, 0));
            assertEquals(0, toRole(OBJECT,    0));

            long alice = Alice(dId);
            assertEquals(encode(Alice(d), dId, SUBJECT), toRole(SUBJECT, alice));
            assertEquals(encode(Alice(d), dId, OBJECT),  toRole(OBJECT, alice));
            assertEquals(NOT_FOUND,                      toRole(PREDICATE, alice));

            long bob = Bob(dId);
            assertEquals(bob,       toRole(SUBJECT, bob));
            assertEquals(NOT_FOUND, toRole(OBJECT, bob));
            assertEquals(NOT_FOUND, toRole(PREDICATE, bob));

            long charlie = charlie(dId);
            assertEquals(charlie,   toRole(OBJECT, charlie));
            assertEquals(NOT_FOUND, toRole(SUBJECT, charlie));
            assertEquals(NOT_FOUND, toRole(PREDICATE, charlie));

            long knows = knows(dId);
            assertEquals(NOT_FOUND, toRole(OBJECT, knows));
            assertEquals(NOT_FOUND, toRole(SUBJECT, knows));
            assertEquals(knows,     toRole(PREDICATE, knows));
        } finally {
            IdAccess.release(dId);
        }
    }

     @RepeatedTest(2) void testAsPlainIn() {
         Dictionary d1 = dummyDict();
         Dictionary d2 = dummyDict();
         int dId1 = register(d1), dId2 = register(d2);
         try {
             long   alice1 = Alice(dId1),     bob1 = Bob(dId1);
             long charlie1 = charlie(dId1), knows1 = knows(dId1);
             assertEquals(d2.stringToId(ALICE_S, SUBJECT), plainIn(d2, SUBJECT, alice1));
             assertEquals(d2.stringToId(ALICE_S,  OBJECT), plainIn(d2,  OBJECT, alice1));

             assertEquals(d1.stringToId(ALICE_S, SUBJECT), plainIn(d1, SUBJECT, alice1));
             assertEquals(d1.stringToId(ALICE_S,  OBJECT), plainIn(d1,  OBJECT, alice1));

             assertEquals(d2.stringToId(BOB_S,     SUBJECT), plainIn(d2, SUBJECT, bob1));
             assertEquals(d2.stringToId(CHARLIE_S,  OBJECT), plainIn(d2,  OBJECT, charlie1));
             assertEquals(d2.stringToId(KNOWS_S,    OBJECT), plainIn(d2,  OBJECT, knows1));

             assertEquals(d1.stringToId(BOB_S,     SUBJECT), plainIn(d1, SUBJECT, bob1));
             assertEquals(d1.stringToId(CHARLIE_S,  OBJECT), plainIn(d1,  OBJECT, charlie1));
             assertEquals(d1.stringToId(KNOWS_S,    OBJECT), plainIn(d1,  OBJECT, knows1));

             assertEquals(0, plainIn(d1, SUBJECT, 0));
             assertEquals(0, plainIn(d1, PREDICATE, 0));
             assertEquals(0, plainIn(d1, OBJECT, 0));

             assertEquals(-1, plainIn(d1, PREDICATE, alice1));
             assertEquals(-1, plainIn(d2, PREDICATE, alice1));
             assertEquals(-1, plainIn(d1, OBJECT, bob1));
             assertEquals(-1, plainIn(d2, OBJECT, bob1));
             assertEquals(-1, plainIn(d1, PREDICATE, bob1));
             assertEquals(-1, plainIn(d2, PREDICATE, bob1));

             assertEquals(-1, plainIn(d1, SUBJECT, charlie1));
             assertEquals(-1, plainIn(d2, SUBJECT, charlie1));
             assertEquals(-1, plainIn(d1, PREDICATE, charlie1));
             assertEquals(-1, plainIn(d2, PREDICATE, charlie1));

             assertEquals(-1, plainIn(d1, SUBJECT, charlie1));
             assertEquals(-1, plainIn(d2, SUBJECT, charlie1));
             assertEquals(-1, plainIn(d1, PREDICATE, charlie1));
             assertEquals(-1, plainIn(d2, PREDICATE, charlie1));
         } finally {
             release(dId1);
             release(dId2);
         }
     }

     @Test void testAsPlainInConcurrent() throws Exception {
        virtualRepeatAndWait("testAsPlainIn", 100, this::testAsPlainIn);
     }

     @Test void testToTerm() {
        var d = dummyDict();
        int dId = register(d);
        int invalid = register(dummyDict());
        release(invalid);
        try {
            assertEquals(ALICE_T,   toTerm(Alice(dId)));
            assertEquals(BOB_T,     toTerm(Bob(dId)));
            assertEquals(CHARLIE_T, toTerm(charlie(dId)));
            assertEquals(KNOWS_T,   toTerm(knows(dId)));

            assertNull(toTerm(0));

            assertThrows(IllegalArgumentException.class, () -> toTerm(NOT_FOUND));
            assertThrows(NoDictException.class, () -> toTerm(encode(1, invalid, SUBJECT)));
            assertThrows(PlainIdException.class, () -> toTerm(4));
        } finally {
            release(dId);
        }
     }

}