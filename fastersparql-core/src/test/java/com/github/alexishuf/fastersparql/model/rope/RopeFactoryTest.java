package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.asFinal;
import static com.github.alexishuf.fastersparql.model.rope.RopeFactory.make;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RopeFactoryTest {

    private static final String PREFIX = "０１２３４５６７８９";
    private static final String PADD_PREFIX = "»０１２３４５６７８９«";
    private static final int PADD_PREFIX_BEGIN = 1;
    private static final int PADD_PREFIX_END = PADD_PREFIX.length()-1;
    private static final String[] DIGIT_STRINGS
            = "0123456789".chars().mapToObj(Character::toString).toArray(String[]::new);
    private static final Rope[] DIGIT_ROPES = IntStream.range(0, 10)
            .mapToObj(i -> asFinal(Character.toString('0'+i))).toArray(Rope[]::new);
    private static final byte[][] DIGIT_PADD_BYTES = IntStream.range(0, 10)
            .mapToObj(i -> new byte[]{(byte)'x', (byte)('0'+i)})
            .toArray(byte[][]::new);
    private static final int PREFIX_BYTES = RopeFactory.requiredBytes(PREFIX);
    private static final int EXPECTED_BYTES = PREFIX_BYTES+1;

    private static final List<String> EXPECTED_STRINGS;
    static {
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String s = PREFIX+(char)('0'+i);
            for (int j = 0; j < 6; j++)
                expected.add(s);
        }
        EXPECTED_STRINGS = expected;
    }


    @Test void test() {
        List<List<FinalSegmentRope>> ropesLists = new ArrayList<>();
        for (int round = 0; round < 2_000; round++) {
            List<FinalSegmentRope> ropes = new ArrayList<>(10*6);
            for (int i = 0; i < 10; i++) {
                ropes.add(make(EXPECTED_BYTES).add(PREFIX).add(DIGIT_STRINGS[i]).take());
                ropes.add(make(EXPECTED_BYTES).add(PREFIX).add(DIGIT_ROPES[i]).take());
                ropes.add(make(EXPECTED_BYTES).add(PREFIX)
                        .add(DIGIT_PADD_BYTES[i], 1, 2).take());

                ropes.add(make(EXPECTED_BYTES).add(PADD_PREFIX, PADD_PREFIX_BEGIN, PADD_PREFIX_END)
                        .add(DIGIT_STRINGS[i]).take());
                ropes.add(make(EXPECTED_BYTES).add(PADD_PREFIX, PADD_PREFIX_BEGIN, PADD_PREFIX_END)
                        .add(DIGIT_ROPES[i]).take());
                ropes.add(make(EXPECTED_BYTES).add(PADD_PREFIX, PADD_PREFIX_BEGIN, PADD_PREFIX_END)
                        .add(DIGIT_PADD_BYTES[i], 1, 2).take());
            }
            ropesLists.add(ropes);
        }
        for (List<FinalSegmentRope> actual : ropesLists) {
            List<String> strings = actual.stream().map(Objects::toString).toList();
            assertEquals(EXPECTED_STRINGS, strings);
        }
    }


    @Test void parallelTest() throws Exception {
        int threads = Runtime.getRuntime().availableProcessors();
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            tasks.repeat(threads, this::test);
        }
        // test() generates a lot of garbage, collecting now reduces interference on future tests
        System.gc();
    }


}