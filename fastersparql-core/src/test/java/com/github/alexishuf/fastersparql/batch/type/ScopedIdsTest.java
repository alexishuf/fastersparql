package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.sparql.expr.PooledTermView;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermView;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.ScopedIds.*;
import static com.github.alexishuf.fastersparql.batch.type.SharedKind.*;
import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.EMPTY_SEGMENT;
import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.EMPTY_UTF8;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ScopedIdsTest {
    private record Id2Term(long id, FinalTerm expected) {}

    private static final ScopedIds.Scope sharedScope = ScopedIds.allocScope().takeOwnership(ScopedIdsTest.class);
    private static final List<Id2Term> allMapped = new ArrayList<>();

    @AfterAll
    static void afterAll() {
        for (Id2Term e : allMapped)
            check(e.id, e.expected);
        sharedScope.recycle(ScopedIdsTest.class);
    }

    private static void check(long id, @Nullable Term expected) {
        var localView = PooledSegmentRopeView.ofEmpty();
        assertEquals(localView(id, localView), expected != null);
        var ropeView = PooledTwoSegmentRope.ofEmpty();
        assertEquals(view(id, ropeView), expected != null);
        var termView = PooledTermView.ofEmptyString();
        assertEquals(view(id, termView), expected != null);

        assertEquals(localLen(id) + ScopedIds.shared(id).len,
                     expected == null ? 0 : expected.len);
        assertEquals(expected, asTerm(id));
        if (expected == null) {
            assertEquals(id, 0L);
        } else {
            assertNotEquals(id, 0L);
            assertFalse(ScopedIds.isEmpty(id), "empty ID, non-empty expected");
            //noinspection AssertBetweenInconvertibleTypes
            assertEquals(expected, ropeView);
            assertSame(localView.segment, localSeg(id));
            assertSame(localView.utf8, localU8(id));
            assertEquals(localView.len, localLen(id));
            assertTrue(localView.has(0, localRope(id), localOff(id),
                                     localOff(id)+localLen(id)));
            assertEquals(expected, termView);
            assertTrue(ScopedIds.equals(id, id));
            assertEquals(0, compare(id, id));
            if (expected.sharedSuffixed()) {
                assertTrue(ropeView.has(0, localView));
                assertTrue(termView.has(0, localView));
                assertTrue(ropeView.has(localView.len, ScopedIds.shared(id)));
                assertTrue(termView.has(localView.len, ScopedIds.shared(id)));
            } else {
                assertTrue(ropeView.has(ScopedIds.shared(id).len, localView));
                assertTrue(termView.has(ScopedIds.shared(id).len, localView));
                assertTrue(ropeView.has(0, ScopedIds.shared(id)));
                assertTrue(termView.has(0, ScopedIds.shared(id)));
            }
            assertEquals(hash(id), expected.hashCode());
            assertEquals(hash(23, id), expected.hash(23));
        }

        byte[] undef = "UNDEF".getBytes(StandardCharsets.UTF_8);
        var acSparql   = PooledMutableRope.getWithCapacity(8);
        var acNT       = PooledMutableRope.getWithCapacity(4);
        var exSparql   = PooledMutableRope.getWithCapacity(8);
        var exNT       = PooledMutableRope.getWithCapacity(4);
        var acNTOrNull = PooledMutableRope.getWithCapacity(4);
        var exNTOrNull = PooledMutableRope.getWithCapacity(4);
        PrefixAssigner assigner = PrefixAssigner.create().takeOwnership(ScopedIdsTest.class);
        try {
            if (expected == null) {
                exNTOrNull.append(undef);
            } else {
                exNT.append(expected);
                exNTOrNull.append(expected);
                expected.toSparql(exSparql, assigner);
            }
            ScopedIds.write(acNT, id, 0, ScopedIds.len(id));
            ScopedIds.writeNT(acNTOrNull, id, undef);
            ScopedIds.writeSparql(acSparql, id, assigner);
        } finally {
            assigner.recycle(ScopedIdsTest.class);
        }
        assertEquals(exSparql,   acSparql);
        assertEquals(exNT,       acNT);
        assertEquals(exNTOrNull, acNTOrNull);

        localView .close();
        termView  .close();
        ropeView  .close();
        acSparql  .close();
        acNT      .close();
        acNTOrNull.close();
        exSparql  .close();
        exNT      .close();
        exNTOrNull.close();
    }

    public static Stream<Arguments> testPutTerm() {
        return Arrays.stream("""
                "plain"
                ""
                "tagged"@en
                "tagged-dash"@en-US
                "loooooooo nnnnnnnnnnnnnn ggggggggggggg"
                "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"@en-US
                "23"^^<http://www.w3.org/2001/XMLSchema#integer>
                "23"^^<http://www.w3.org/2001/XMLSchema#int>
                "23.0"^^<http://www.w3.org/2001/XMLSchema#double>
                "2.3e1"^^<http://www.w3.org/2001/XMLSchema#decimal>
                <relative>
                <looooooooooooooooooooooooooooooooooooooooooooooong_relative>
                <http://www.w3.org/2001/XMLSchema#>
                <http://www.w3.org/2001/XMLSchema#int>
                <http://www.w3.org/2001/XMLSchema#gYear>
                <http://www.example.org/ns#Alice>
                <http://www.example.org/ns#John%20Doe>
                <http://overflow.example.org/namespace/01/local/01>
                <http://overflow.example.org/namespace/02/local/01>
                <http://overflow.example.org/namespace/03/local/01>
                <http://overflow.example.org/namespace/04/local/01>
                <http://overflow.example.org/namespace/05/local/01>
                <http://overflow.example.org/namespace/06/local/01>
                <http://overflow.example.org/namespace/07/local/01>
                <http://overflow.example.org/namespace/08/local/01>
                <http://overflow.example.org/namespace/09/local/01>
                <http://overflow.example.org/namespace/10/local/01>
                <http://overflow.example.org/namespace/11/local/01>
                <http://overflow.example.org/namespace/12/local/01>
                <http://overflow.example.org/namespace/13/local/01>
                <http://overflow.example.org/namespace/14/local/01>
                <http://overflow.example.org/namespace/15/local/01>
                <http://overflow.example.org/namespace/16/local/01>
                <http://overflow.example.org/namespace/17/local/01>
                <http://overflow.example.org/namespace/18/local/01>
                <http://overflow.example.org/namespace/19/local/01>
                <http://overflow.example.org/namespace/20/local/01>
                <http://overflow.example.org/namespace/21/local/01>
                <http://overflow.example.org/namespace/22/local/01>
                <http://overflow.example.org/namespace/23/local/01>
                <http://overflow.example.org/namespace/24/local/01>
                <http://overflow.example.org/namespace/25/local/01>
                <http://overflow.example.org/namespace/26/local/01>
                <http://overflow.example.org/namespace/27/local/01>
                <http://overflow.example.org/namespace/28/local/01>
                <http://overflow.example.org/namespace/29/local/01>
                <http://overflow.example.org/namespace/30/local/01>
                <http://overflow.example.org/namespace/31/local/01>
                <http://overflow.example.org/namespace/32/local/01>
                <http://overflow.example.org/namespace/33/local/01>
                <http://overflow.example.org/namespace/34/local/01>
                <http://overflow.example.org/namespace/35/local/01>
                <http://overflow.example.org/namespace/36/local/01>
                <http://overflow.example.org/namespace/37/local/01>
                <http://overflow.example.org/namespace/38/local/01>
                <http://overflow.example.org/namespace/39/local/01>
                <http://overflow.example.org/namespace/40/local/01>
                <http://overflow.example.org/namespace/41/local/01>
                <http://overflow.example.org/namespace/42/local/01>
                <http://overflow.example.org/namespace/43/local/01>
                <http://overflow.example.org/namespace/44/local/01>
                <http://overflow.example.org/namespace/45/local/01>
                <http://overflow.example.org/namespace/46/local/01>
                <http://overflow.example.org/namespace/47/local/01>
                <http://overflow.example.org/namespace/48/local/01>
                <http://overflow.example.org/namespace/49/local/01>
                <http://overflow.example.org/namespace/01/local/02>
                <http://overflow.example.org/namespace/02/local/02>
                <http://overflow.example.org/namespace/03/local/02>
                <http://overflow.example.org/namespace/04/local/02>
                <http://overflow.example.org/namespace/05/local/02>
                <http://overflow.example.org/namespace/06/local/02>
                <http://overflow.example.org/namespace/07/local/02>
                <http://overflow.example.org/namespace/08/local/02>
                <http://overflow.example.org/namespace/09/local/02>
                <http://overflow.example.org/namespace/10/local/02>
                <http://overflow.example.org/namespace/11/local/02>
                <http://overflow.example.org/namespace/12/local/02>
                <http://overflow.example.org/namespace/13/local/02>
                <http://overflow.example.org/namespace/14/local/02>
                <http://overflow.example.org/namespace/15/local/02>
                <http://overflow.example.org/namespace/16/local/02>
                <http://overflow.example.org/namespace/17/local/02>
                <http://overflow.example.org/namespace/18/local/02>
                <http://overflow.example.org/namespace/19/local/02>
                <http://overflow.example.org/namespace/20/local/02>
                <http://overflow.example.org/namespace/21/local/02>
                <http://overflow.example.org/namespace/22/local/02>
                <http://overflow.example.org/namespace/23/local/02>
                <http://overflow.example.org/namespace/24/local/02>
                <http://overflow.example.org/namespace/25/local/02>
                <http://overflow.example.org/namespace/26/local/02>
                <http://overflow.example.org/namespace/27/local/02>
                <http://overflow.example.org/namespace/28/local/02>
                <http://overflow.example.org/namespace/29/local/02>
                <http://overflow.example.org/namespace/30/local/02>
                <http://overflow.example.org/namespace/31/local/02>
                <http://overflow.example.org/namespace/32/local/02>
                <http://overflow.example.org/namespace/33/local/02>
                <http://overflow.example.org/namespace/34/local/02>
                <http://overflow.example.org/namespace/35/local/02>
                <http://overflow.example.org/namespace/36/local/02>
                <http://overflow.example.org/namespace/37/local/02>
                <http://overflow.example.org/namespace/38/local/02>
                <http://overflow.example.org/namespace/39/local/02>
                <http://overflow.example.org/namespace/40/local/02>
                <http://overflow.example.org/namespace/41/local/02>
                <http://overflow.example.org/namespace/42/local/02>
                <http://overflow.example.org/namespace/43/local/02>
                <http://overflow.example.org/namespace/44/local/02>
                <http://overflow.example.org/namespace/45/local/02>
                <http://overflow.example.org/namespace/46/local/02>
                <http://overflow.example.org/namespace/47/local/02>
                <http://overflow.example.org/namespace/48/local/02>
                <http://overflow.example.org/namespace/49/local/02>
                _:bnode1
                _:loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong_
                _:a
                _:1
                _:
                """.split("\n")).map(nt -> arguments(Term.valueOf(nt)));
    }

    @ParameterizedTest @MethodSource
    public void testPutTerm(FinalTerm t) {
        TermView view = new TermView();
        view.wrap(t);
        var sr = RopeFactory.make(t.len).add(t).take();
        var tsr = new TwoSegmentRope();
        tsr.wrapFirst(t.shared());
        tsr.wrapSecond(t.local());
        if (t.sharedSuffixed())
            tsr.flipSegments();

        long idTermLocal,  idViewLocal,  idSRLocal,  idTSRLocal, idPairLocal;
        long idTermShared, idViewShared, idSRShared, idTSRShared, idPairShared;
        var localScope = ScopedIds.allocScope().takeOwnership(this);
        try {
            idTermLocal  = localScope .put(t  );
            idViewLocal  = localScope .put(t  );
            idSRLocal    = localScope .put(sr );
            idTSRLocal   = localScope .put(tsr);
            idPairLocal  = localScope.put(t.finalShared(), t.local(), WHOLE_UNKNOWN);

            idSRShared   = sharedScope.put(sr );
            idPairShared = sharedScope.put(t.finalShared(), t.local(), WHOLE_UNKNOWN);
            idTSRShared  = sharedScope.put(tsr);
            idTermShared = sharedScope.put(t  );
            idViewShared = sharedScope.put(t  );

            check(idTermLocal,  t);
            check(idViewLocal,  t);
            check(idSRLocal,    t);
            check(idTSRLocal,   t);
            check(idPairLocal,  t);
            check(idTermShared, t);
            check(idViewShared, t);
            check(idSRShared,   t);
            check(idTSRShared,  t);
            check(idPairShared, t);
        } finally {
            localScope.recycle(this);
        }
        // IDs on local scope are not valid anymore
        assertThrows(ScopedIds.ClosedScope.class, () -> ScopedIds.localSeg(idTermLocal));
        assertThrows(ScopedIds.ClosedScope.class, () -> ScopedIds.localU8(idViewLocal));
        assertThrows(ScopedIds.ClosedScope.class, () -> ScopedIds.localSeg(idSRLocal));
        assertThrows(ScopedIds.ClosedScope.class, () -> ScopedIds.localU8(idTSRLocal));
        assertThrows(ScopedIds.ClosedScope.class, () -> ScopedIds.localSeg(idPairLocal));

        // IDs on shared scope remain valid
        check(idTermShared, t);
        check(idViewShared, t);
        check(idSRShared,   t);
        check(idTSRShared,  t);
        check(idPairShared, t);
        allMapped.add(new Id2Term(idTermShared, t));
        allMapped.add(new Id2Term(idViewShared, t));
        allMapped.add(new Id2Term(idSRShared, t));
        allMapped.add(new Id2Term(idTSRShared, t));
        allMapped.add(new Id2Term(idPairShared, t));
    }


    @ParameterizedTest @ValueSource(ints = {0, 1, 2, 3, 4})
    void testPutManyTimes(int variant) {
        var list = testPutTerm().map(a -> (FinalTerm) a.get()[0]).toList();
        var s = allocScope().takeOwnership(this);
        try {
            putByVariant(variant, list, s, 1_000);
        } finally {
            s.recycle(this);
            System.gc();
        }
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 2, 3, 4})
    void testPutConcurrent(int variant) throws Exception {
        var list = testPutTerm().map(a -> (FinalTerm) a.get()[0]).toList();
        var s = allocScope().takeOwnership(this);
        try {
            TestTaskSet.platformRepeatAndWait(getClass().getSimpleName(),
                    Runtime.getRuntime().availableProcessors(),
                    () -> putByVariant(variant, list, s, 100));
        } finally {
            s.recycle(this);
            System.gc();
        }
    }

    private static void putByVariant(int variant, List<FinalTerm> list, Scope s, int rounds) {
        List<Id2Term> mapped = new ArrayList<>((rounds+1)*list.size());
        try (var tmp = PooledMutableRope.get()) {
            for (FinalTerm t : list) {
                mapped.add(new Id2Term(switch (variant) {
                    case 0 -> s.put(t);
                    case 1 -> s.put(tmp.clear().append(t));
                    case 2 -> s.put(t.finalShared(), t.local(), WHOLE_UNKNOWN);
                    case 3 -> s.put(t.finalShared(), t.local(),
                            t.sharedSuffixed() ? SharedKind.SUFF_LIT : WHOLE_UNKNOWN);
                    case 4 -> s.put(t.finalShared(), t.local(), t.sharedKind());
                    default -> throw new IllegalArgumentException("unexpected variant");
                }, t));
            }
            for (int i = 0; i < rounds; i++) {
                for (FinalTerm t : list) {
                    mapped.add(new Id2Term(switch (i % 5) {
                        case 0 -> s.put(t);
                        case 1 -> s.put(tmp.clear().append(t));
                        case 2 -> s.put(t.finalShared(), t.local(), WHOLE_UNKNOWN);
                        case 3 -> s.put(t.finalShared(), t.local(),
                                t.sharedSuffixed() ? SharedKind.SUFF_LIT : WHOLE_UNKNOWN);
                        case 4 -> s.put(t.finalShared(), t.local(), t.sharedKind());
                        default -> throw new IllegalArgumentException("unexpected variant");
                    }, t));
                }
            }
        }
        for (int i = 0; i < 2; i++) {
            for (Id2Term e : mapped)
                check(e.id, e.expected);
            System.gc();
        }
    }

    @Test
    public void testPutNull() {
        var s = ScopedIds.allocScope().takeOwnership(this);
        try {
            check(s.put((Term)null), null);
            check(s.put((FinalTerm)null), null);
            check(s.put((TermView)null), null);
            check(s.put((FinalSegmentRope)null), null);
            check(s.put((SegmentRope)null), null);
            check(s.put((TwoSegmentRope)null), null);
            check(s.put(null, EMPTY, WHOLE_UNKNOWN), null);
            check(s.put(null, EMPTY, WHOLE_LIT), null);
            check(s.put(EMPTY, EMPTY, WHOLE_IRI_OR_BLANK), null);
            check(s.put(EMPTY, EMPTY, SUFF_LIT), null);
            check(s.put(null, EMPTY_SEGMENT, EMPTY_UTF8, 23, 0, WHOLE_UNKNOWN), null);
            check(s.put(null, EMPTY_SEGMENT, EMPTY_UTF8, 0, 0, SUFF_LIT), null);
            check(s.put(null, EMPTY_SEGMENT, null, 0, 0, WHOLE_UNKNOWN), null);
            check(s.put(null, EMPTY_SEGMENT, null, 27, 0, SUFF_LIT), null);
            check(s.put(EMPTY, EMPTY_SEGMENT, EMPTY_UTF8, 23, 0, WHOLE_UNKNOWN), null);
            check(s.put(EMPTY, EMPTY_SEGMENT, EMPTY_UTF8, 0, 0, SUFF_LIT), null);
            check(s.put(EMPTY, EMPTY_SEGMENT, null, 0, 0, WHOLE_UNKNOWN), null);
            check(s.put(EMPTY, EMPTY_SEGMENT, null, 27, 0, SUFF_LIT), null);
        } finally {
            s.recycle(this);
        }

    }

}