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

import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.operators.MergeBIt;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.L_Result;

import java.util.List;

import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static java.util.stream.IntStream.range;

// See jcstress-samples or existing tests for API introduction and testing guidelines

@JCStressTest
// Outline the outcomes here. The default outcome is provided, you need to remove it:
@Outcome(id = "OK", expect = Expect.ACCEPTABLE, desc = "Only allowed outcome.")
@State
public class MergeBItCTest extends BItCTest {
    private final SPSCBIt<TermBatch> cb1;
    private final SPSCBIt<TermBatch> cb2;
    private final TermBatch[] batches1 = {batch(3), batch(4), batch(5)};
    private final TermBatch[] batches2 = {batch(6), batch(7)};

    @SuppressWarnings("resource") private static MergeBIt<TermBatch> makeMerge() {
        List<TermBatch> itBatches = List.of(batch(1), batch(2));
        List<BIt<TermBatch>> sources = List.of(
                new IteratorBIt<>(itBatches, TERM, X).maxBatch(1),
                new SPSCBIt<>(TERM, X, 2),
                new SPSCBIt<>(TERM, X, 2)
        );
        MergeBIt<TermBatch> it = new MergeBIt<>(sources, TERM, X);
        it.maxReadyItems(2);
        return it;
    }

    public MergeBItCTest() {
        super(makeMerge(), range(1, 8).mapToObj(BItCTest::batch).toArray(TermBatch[]::new));
        var sources = ((MergeBIt<TermBatch>) it).sources();
        cb1 = (SPSCBIt<TermBatch>) sources.get(1);
        cb2 = (SPSCBIt<TermBatch>) sources.get(2);
    }

    @Actor public void consumer () { consumeToCompletion(); }
    @Actor public void producer1() { produceAndComplete(cb1, batches1); }
    @Actor public void producer2() { produceAndComplete(cb2, batches2); }

    @Arbiter public void arbiter(L_Result r) {
        StringBuilder desc = new StringBuilder(16);
        // check size
        if (consumedSize < 7)
            desc.append("MISSING,");

        // check for duplicates
        for (int i = 0, prev; i < consumedSize; i++) {
            prev = consumed[i];
            for (int j = i+1; j < consumedSize; j++) {
                if (consumed[j] == prev)
                    desc.append("DUP(").append(consumed[i]).append("),");
            }
        }

        // check for unexpected items
        for (int i = 0; i < consumedSize; i++) {
            int c = consumed[i];
            if (c < 1 || c > 8) desc.append("UNEXPECTED(").append(c).append("),");
        }

        // check intra-source order
        var idx = new int[9];
        for (int i = 0; i < consumedSize; i++) {
            int c = consumed[i];
            if (c > 0 && c < 9)
                idx[c] = i;
        }
        if (idx[2] < idx[1]) desc.append("2 BFR 1,");
        if (idx[4] < idx[3]) desc.append("4 BFR 3,");
        if (idx[5] < idx[4]) desc.append("5 BFR 4,");
        if (idx[7] < idx[6]) desc.append("7 BFR 6");

        if (desc.isEmpty()) desc.append("OK");
        r.r1 =  desc.toString();
    }

    public static void main(String[] args) throws Exception {
        MergeBItCTest t = new MergeBItCTest();
        Thread t1 = new Thread(t::producer1);
        Thread t2 = new Thread(t::producer2);
        t1.start();
        t2.start();
        t.consumer();
        t1.join();
        t2.join();
        L_Result r = new L_Result();
        t.arbiter(r);
        System.out.println(r.r1);
    }
}
