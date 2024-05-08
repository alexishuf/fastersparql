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
package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.L_Result;

import java.util.List;

import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;

// See jcstress-samples or existing tests for API introduction and testing guidelines

@JCStressTest
// Outline the outcomes here. The default outcome is provided, you need to remove it:
@Outcome(id = "0,1,2,3,4,5,6,7", expect = Expect.ACCEPTABLE, desc = "Only allowed outcome.")
@State
public class QueueWrappingSPSCUnitBItCTest extends BItCTest {
    @SuppressWarnings("unchecked") private static Orphan<TermBatch>[] makeBatches() {
        var batches = new Orphan[8];
        for (int i = 0; i < batches.length; i++)
            batches[i] = TermBatch.of(List.of(INTS[i]));
        return batches;
    }

    public QueueWrappingSPSCUnitBItCTest() {
        super(new SPSCBIt<>(TERM, X, 1), makeBatches());
    }

    @Actor   public void producer()           {  produceAndComplete(); }
    @Actor   public void consumer()           { consumeToCompletion(); }
    @Arbiter public void  arbiter(L_Result r) { r.r1 =  buildResult(); }
}
