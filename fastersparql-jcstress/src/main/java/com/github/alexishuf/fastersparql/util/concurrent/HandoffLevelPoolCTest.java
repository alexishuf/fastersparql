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
import org.openjdk.jcstress.infra.results.IIII_Result;

@JCStressTest
@Outcome(id = "0, 0, 11, 12", expect = Expect.ACCEPTABLE, desc = "Only allowed")
@State
public class HandoffLevelPoolCTest {
    private static final int CAPACITY = 32;
    private static final Integer i0 = 0, i11 = 11, i12 = 12;

    private final LevelPool<Integer> pool = new LevelPool<>(Integer.class, 1, 1, 1, 2);

    @Actor public void producer1(IIII_Result r) {
        var retained = pool.offer(i11, CAPACITY);
        r.r1 = retained == null ? i0 : i11;
    }
    @Actor public void producer2(IIII_Result r) {
        var retained = pool.offer(i12, CAPACITY);
        r.r2 = retained == null ? i0 : i12;
    }
    @Actor public void consumer1(IIII_Result r) {
        Integer i = null;
        while (i == null)  i = pool.getExact(1);
        r.r3 = pool.getExact(1) == null ? i : i0;
    }
    @Actor public void consumer2(IIII_Result r) {
        Integer i = null;
        while (i == null)  i = pool.getExact(2);
        r.r4 = pool.getExact(2) == null ? i : i0;
    }
}
