package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;

import java.util.Objects;

import static com.github.alexishuf.fastersparql.util.CSUtils.BASE64_2_BITS;
import static com.github.alexishuf.fastersparql.util.CSUtils.BITS_2_BASE64;

public final class WsBindingSeq {
    public static final ByteRope VAR = new ByteRope("fastersparqlBindingSeq");
    private static final int LIT_LEN_SHORT = 1  /*"*/
                                           + 2  /* 12 bits unsigned big endian in base64 */
                                           + 1; /*"*/
    private static final int LIT_LEN_LONG = 1  /*"*/
                                     + 8  /* 48 bits unsigned big endian in base64 */
                                     + 1; /*"*/
    private static final String BAD_LEN_MSG = "Value for ?"+VAR+" does not have length == "+ LIT_LEN_LONG;

    private final byte[] tmp;

    public WsBindingSeq() {
        tmp = new byte[LIT_LEN_LONG];
        tmp[0] = '"';
        tmp[LIT_LEN_LONG -1] = '"';
    }

    public <B extends Batch<B>> void write(long seq, B batch, int col) {
        int len;
        if (seq < 0) {
            throw new IllegalArgumentException("seq cannot be negative");
        } else if (seq < 0xfff) {
            tmp[1] = BITS_2_BASE64[((int)seq>>6)&0x3f];
            tmp[2] = BITS_2_BASE64[ (int)seq    &0x3f];
            tmp[3] = '"';
            len = 4;
        } else {
            tmp[1] = BITS_2_BASE64[(int) ((seq >> 42) & 0x3f)];
            tmp[2] = BITS_2_BASE64[(int) ((seq >> 36) & 0x3f)];
            tmp[3] = BITS_2_BASE64[(int) ((seq >> 30) & 0x3f)];
            tmp[4] = BITS_2_BASE64[(int) ((seq >> 24) & 0x3f)];
            tmp[5] = BITS_2_BASE64[(int) ((seq >> 18) & 0x3f)];
            tmp[6] = BITS_2_BASE64[(int) ((seq >> 12) & 0x3f)];
            tmp[7] = BITS_2_BASE64[(int) ((seq >>  6) & 0x3f)];
            tmp[8] = BITS_2_BASE64[(int) ( seq        & 0x3f)];
            len = 10;
        }
        batch.putTerm(col, ByteRope.EMPTY, tmp, 0, len, true);
    }

    public Term toTerm(long seq) {
        CompressedBatch batch = Batch.COMPRESSED.createSingleton(1);
        batch.beginPut();
        write(seq, batch, 0);
        batch.commitPut();
        Term term = Objects.requireNonNull(batch.get(0, 0));
        Batch.COMPRESSED.recycle(batch);
        return term;
    }

    public static long parse(SegmentRope buf, int begin, int end) {
        byte bits1 = BASE64_2_BITS[buf.get(begin+1)];
        byte bits2 = BASE64_2_BITS[buf.get(begin+2)];
        if (end-begin == LIT_LEN_SHORT) {
            return (bits1 << 6) | bits2;
        } else if (end-begin == LIT_LEN_LONG) {
            return  ((long) bits1 << 42)
                  | ((long) bits2 << 36)
                  | ((long)BASE64_2_BITS[buf.get(begin+3)] << 30)
                  | (      BASE64_2_BITS[buf.get(begin+4)] << 24)
                  | (      BASE64_2_BITS[buf.get(begin+5)] << 18)
                  | (      BASE64_2_BITS[buf.get(begin+6)] << 12)
                  | (      BASE64_2_BITS[buf.get(begin+7)] <<  6)
                  | (      BASE64_2_BITS[buf.get(begin+8)]      );
        } else {
            throw new InvalidSparqlResultsException(BAD_LEN_MSG);
        }
    }
}

