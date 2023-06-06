package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WsBindingSeqTest {
    private final WsBindingSeq encoder = new WsBindingSeq();

    @ParameterizedTest @ValueSource(longs = {
            0, 1, 2, 3, 63, 64, 65, 127, 128, 255, 256, 257,
            Integer.MAX_VALUE,
            0xffffffffL,
            0xffffffffffL,
            0xffffffffffffL
    })
    void test(long seq) {
        CompressedBatch batch = Batch.COMPRESSED.createSingleton(2);
        batch.beginPut();
        encoder.write(seq, batch, 0);
        encoder.write(seq, batch, 1);
        batch.commitPut();
        assertEquals(seq, WsBindingSeq.parse(requireNonNull(batch.get(0, 0)).local(), 0, batch.localLen(0, 0)));
        assertEquals(seq, WsBindingSeq.parse(requireNonNull(batch.get(0, 0)).local(), 0, batch.localLen(0, 0)));
    }

}