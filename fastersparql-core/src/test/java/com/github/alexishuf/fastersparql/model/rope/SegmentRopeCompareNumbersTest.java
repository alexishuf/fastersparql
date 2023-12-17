package com.github.alexishuf.fastersparql.model.rope;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.MemorySegment;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.SegmentRope.compareNumbers;
import static java.lang.Integer.signum;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class SegmentRopeCompareNumbersTest {

    static Stream<Arguments> test() {
        return Stream.of(
                arguments("",  "",   0),
                arguments("1", "1",  0),
                arguments("1", "2", -1),
                arguments("2", "1",  1),
                arguments("2", "2",  0),
                arguments("2.0", "2",  0),

                arguments("12", "13", -1),
                arguments("12", "10",  1),
                arguments("9", "10", -1),
                arguments("23", "23", 0),
                arguments("23", "23.0", 0),
                arguments("23", "23.00", 0),
                arguments("23.0", "23.00", 0),

                arguments("123", "133", -1),
                arguments("999", "989",  1),

                arguments("01", "1", 0),
                arguments("1", "01", 0),
                arguments("02", "002", 0),
                arguments("02", "+02", 0),
                arguments("01", "+02", -1),
                arguments("09", "+01",  1),
                arguments("09", "+010", -1),

                arguments("1.0", "1.0",  0),
                arguments("1.1", "1.0",  1),
                arguments("1.0", "1.2", -1),

                arguments("741.23", "741.230", 0),
                arguments("741.23", "741.2300000000", 0),
                arguments("741.23", "741.231", -1),
                arguments("741.23", "741.2301", -1),
                arguments("741.231", "741.230",  1),
                arguments("741.2301", "741.2300",  1)
        );
    }

    @ParameterizedTest @MethodSource
    void test(String l, String r, int expected) {
        test0(l, 0, l.length(), r, 0, r.length(), expected);

        // reflexivity
        test0(l, 0, l.length(), l, 0, l.length(), 0);
        test0(r, 0, r.length(), r, 0, r.length(), 0);

        // prepend garbage to one side only
        test0("1"+l, 1, l.length(), r, 0, r.length(), expected);
        test0(l, 0, l.length(), "23-"+r, 3, r.length(), expected);

        // append garbage to one side only
        test0(l+"1", 0, l.length(), r, 0, r.length(), expected);
        test0(l, 0, l.length(), r+"30.1", 0, r.length(), expected);

        //wrap both sides in quotes
        test0("\""+l+"\"", 1, l.length(), "\""+r+"\"", 1, r.length(), expected);

        //wrap both sides in mismatched quotes
        test0("\""+l+"\"", 1, l.length(), "'"+r+"'", 1, r.length(), expected);
        test0("'"+l+"'", 1, l.length(), "\""+r+"\"", 1, r.length(), expected);
    }

    private void test0(String l, int lOff, int lLen, String r, int rOff, int rLen, int expected) {
        byte[] lU8 = l.getBytes(UTF_8);
        byte[] rU8 = r.getBytes(UTF_8);
        MemorySegment lSeg = MemorySegment.ofArray(lU8);
        MemorySegment rSeg = MemorySegment.ofArray(rU8);
        SegmentRope lRope = new SegmentRope(lSeg, lU8, lOff, lLen);
        SegmentRope rRope = new SegmentRope(rSeg, rU8, rOff, rLen);
        SegmentRope lAbsRope = new SegmentRope(lSeg, lU8, 0, lU8.length);
        SegmentRope rAbsRope = new SegmentRope(rSeg, rU8, 0, rU8.length);

        assertEquals(expected, signum(compareNumbers(lU8,  lOff, lLen, rU8,  rOff, rLen)));
        assertEquals(expected, signum(compareNumbers(lSeg, lOff, lLen, rSeg, rOff, rLen)));
        assertEquals(expected, signum(compareNumbers(lRope, 0, lRope.len, rRope, 0, rRope.len)));
        assertEquals(expected, signum(compareNumbers(lAbsRope, lOff, lLen, rAbsRope, rOff, rLen)));
    }
}
