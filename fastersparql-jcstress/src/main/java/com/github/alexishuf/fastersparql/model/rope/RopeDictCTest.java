package com.github.alexishuf.fastersparql.model.rope;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;

@JCStressTest
@Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "Only allowed result")
@State
public class RopeDictCTest {
    private static final ByteRope iRope = new ByteRope("\"1\"^^xsd:integer");
    private final ByteRope iriRope = new ByteRope("<http://www.example.org/"+(int)(4*Math.random())+"/jcstress/"+(int)(1_000_000*Math.random())+">");

    int iId1, iId2;
    int pId1, pId2;

    @Actor void iIntern1() { iId1 = (int) RopeDict.internLit(iRope, 0, iRope.len); }
    @Actor void iIntern2() { iId2 = (int) RopeDict.internLit(iRope, 0, iRope.len); }
    @Actor void pIntern1() { pId1 = (int) RopeDict.internLit(iriRope, 0, iriRope.len);}
    @Actor void pIntern2() { pId2 = (int) RopeDict.internLit(iriRope, 0, iriRope.len);}

    @Arbiter void arbiter(II_Result r) {
        r.r1 = iId1 == iId2 ? 1 : 2;
        r.r2 = pId1 == pId2 ? 1 : 2;
    }
}
