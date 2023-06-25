/*
 * Copyright (c) 2017, Red Hat Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.alexishuf.fastersparql.util.concurrent;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.L_Result;

@JCStressTest
@State
@Outcome(id = "0,2 1,0", expect = Expect.ACCEPTABLE, desc = "prod-1 and cons-1 won")
@Outcome(id = "0,2 0,1", expect = Expect.ACCEPTABLE, desc = "prod-1 and cons-2 won")
@Outcome(id = "1,0 2,0", expect = Expect.ACCEPTABLE, desc = "prod-2 and cons-1 won")
@Outcome(id = "1,0 0,2", expect = Expect.ACCEPTABLE, desc = "prod-2 and cons-1 won")

@Outcome(id = "0,0 1,2", expect = Expect.ACCEPTABLE_INTERESTING, desc = "fully serialized")
@Outcome(id = "0,0 2,1", expect = Expect.ACCEPTABLE_INTERESTING, desc = "fully serialized")

@Outcome(id = "0,0 0,1", expect = Expect.ACCEPTABLE, desc = "cons-1 too early")
@Outcome(id = "0,0 0,2", expect = Expect.ACCEPTABLE, desc = "cons-1 too early")
@Outcome(id = "0,0 1,0", expect = Expect.ACCEPTABLE, desc = "cons-2 too early")
@Outcome(id = "0,0 2,0", expect = Expect.ACCEPTABLE, desc = "cons-2 too early")

@Outcome(id = "0,2 0,0", expect = Expect.ACCEPTABLE, desc = "consumers too early, prod-1 won")
@Outcome(id = "1,0 0,0", expect = Expect.ACCEPTABLE, desc = "consumers too early, prod-2 won")
@Outcome(id = "0,0 0,0", expect = Expect.FORBIDDEN, desc = "consumers too early, both producers won")
public class RaceLevelPoolCTest {
    private static final int CAPACITY = 32;
    private static final Integer[] INTS = {0, 1, 2};
    private final LevelPool<Integer> pool = new LevelPool<>(Integer.class, 1, 1, 1, 1);
    private final int[] retained = new int[2];
    private final int[] got = new int[2];

    private void produce(int i) {
        Integer retained = pool.offer(INTS[i], CAPACITY);
        this.retained[i-1] = retained == null ? INTS[0] : retained;
    }
    private void consume(int i) {
        Integer pooled = pool.getAtLeast(CAPACITY);
        this.got[i-1] = pooled == null ? INTS[0] : pooled;
    }

    @Actor public void producer1() { produce(1); }
    @Actor public void producer2() { produce(2); }

    @Actor public void consumer1() { consume(1); }
    @Actor public void consumer2() { consume(2); }

    @Arbiter public void arbiter(L_Result r) {
        var sb = new StringBuilder(3*2*2/*"i,"*/ + 1/*" "*/ + 4/*" DUP"*/);
        for (int i : retained) sb.append(i).append(',');
        sb.setLength(sb.length()-1);
        sb.append(' ');
        for (int i : got) sb.append(i).append(',');
        sb.setLength(sb.length()-1);

        boolean dup = false;
        Integer i0 = pool.getExact(2), i1 = pool.getExact(2);
        for (int idx = 1; idx < 3; idx++) {
            Integer iObj = INTS[idx];
            int iVal = iObj;
            if (got[0] == iVal || got[1] == iVal)  // some consumer fetched
                dup |= iObj.equals(i0) || iObj.equals(i1); // fetched again after consumers
        }
        if (dup) sb.append(" DUP");
        r.r1 = sb.toString();
    }
}
