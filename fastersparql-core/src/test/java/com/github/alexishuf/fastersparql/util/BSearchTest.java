package com.github.alexishuf.fastersparql.util;

import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.BSearch.binarySearch;
import static com.github.alexishuf.fastersparql.util.BSearch.binarySearchLocal;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BSearchTest {

    @ParameterizedTest @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
            15, 16, 17, 22,
            31, 32, 33, 48,
            63, 64, 65, 96,
            1023, 1024, 1025,
    })
    public void test(int size) {
        int[] enumeration = new int[size];
        for (int i = 0; i < size; i++) enumeration[i] = i;
        for (int i = 0; i < size; i++)
            assertEquals(i, binarySearch(enumeration, i));
        assertEquals(-1, binarySearch(enumeration, -1));
        assertEquals(-1, binarySearch(enumeration, -2));
        assertEquals(-(size)-1, binarySearch(enumeration, size));

        if (size > 100)
            return;
        for (int begin = 0; begin < size; begin++) {
            for (int end = begin; end < size ; end++) {
                for (int i = begin; i < end; i++) {
                    assertEquals( Arrays.binarySearch(enumeration, begin, end, i),
                                 binarySearch(enumeration, begin, end, i));
                }
            }
        }

        int[] even = new int[size];
        for (int i = 0; i < size; i++) even[i] = 2 * i;
        for (int i = 0; i < 2*size; i++)
            assertEquals(Arrays.binarySearch(even, i), binarySearch(even, i));
        for (int begin = 0; begin < size; begin++) {
            for (int end = begin; end < size; end++) {
                for (int i = 0; i < 2*size; i++) {
                    assertEquals(Arrays.binarySearch(even, begin, end, i),
                                 binarySearch(even, begin, end, i));
                }
            }
        }

        int[] odd = new int[size];
        for (int i = 0; i < size; i++) odd[i] = 2 * i + 1;
        for (int i = 0; i < 2*size; i++)
            assertEquals(Arrays.binarySearch(odd, i), binarySearch(odd, i));
        for (int begin = 0; begin < size; begin++) {
            for (int end = begin; end < size; end++) {
                for (int i = 0; i < 2*size; i++) {
                    assertEquals(Arrays.binarySearch(odd, begin, end, i),
                                 binarySearch(odd, begin, end, i));
                }
            }
        }
    }

    static Stream<Arguments> testBytes() {
        return Stream.of(
                arguments(List.of()),
                arguments(List.of("1")),
                arguments(List.of("1", "2")),
                arguments(List.of("1", "2", "3")),
                arguments(IntStream.range(0, 32).mapToObj(Integer::toString).toList()),
                arguments(List.of("a1", "a2")),
                arguments(List.of("a2", "b1")),
                arguments(List.of("a2", "b")),
                arguments(List.of("a", "a2")),
                arguments(List.of("a", "a2", "b"))
        );
    }

    @ParameterizedTest @MethodSource
    void testBytes(List<String> strings) {
        List<String> sortedList = strings.stream().sorted().distinct().toList();
        String[] sortedStrings = sortedList.toArray(new String[0]);
        byte[][] sorted = sortedList.stream().map(s -> s.getBytes(UTF_8)).toArray(byte[][]::new);

        for (int i = 0; i < sorted.length; i++) {
            assertEquals(i, binarySearch(sorted, 0, sorted.length, sorted[i]));
            byte[] suffixedL = (new String(sorted[i]) + " ").getBytes(UTF_8);
            byte[] suffixedH = (new String(sorted[i]) + "~").getBytes(UTF_8);
            byte[] prefixedL = (" " + new String(sorted[i])).getBytes(UTF_8);
            byte[] prefixedH = ("~" + new String(sorted[i])).getBytes(UTF_8);

            int exSuffixedL = Arrays.binarySearch(sortedStrings, new String(suffixedL));
            int exSuffixedH = Arrays.binarySearch(sortedStrings, new String(suffixedH));
            int exPrefixedL = Arrays.binarySearch(sortedStrings, new String(prefixedL));
            int exPrefixedH = Arrays.binarySearch(sortedStrings, new String(prefixedH));
            assertEquals(exSuffixedL, binarySearch(sorted, 0, sorted.length, suffixedL));
            assertEquals(exSuffixedH, binarySearch(sorted, 0, sorted.length, suffixedH));
            assertEquals(exPrefixedL, binarySearch(sorted, 0, sorted.length, prefixedL));
            assertEquals(exPrefixedH, binarySearch(sorted, 0, sorted.length, prefixedH));
        }
    }

    static Stream<Arguments> testBinarySearchLocal() {
        List<String> localPrefixes = List.of("", "a", "abcdefghijklmnopqrstuvxwyzABCDE", "abcdefghijklmnopqrstuvxwyzABCDEF");
        List<Integer> sizes = List.of(
                0, 1, 2, 3, 4,
                7, 8, 9,
                15, 16, 17, 23,
                31, 32, 33
        );
        List<Arguments> list = new ArrayList<>();
        for (String localPrefix : localPrefixes) {
            for (int size : sizes)
                list.add(arguments(localPrefix, size));
        }
        return list.stream();
    }

    @ParameterizedTest  @MethodSource
    void testBinarySearchLocal(String localPrefix, int size) {
        String[] strings = new String[size];
        Term[] sorted = new Term[size];
        for (int i = 0; i < sorted.length; i++) {
            var uri = String.format("<http://www.example.org/ns#%s%03d>", localPrefix, i);
            sorted[i] = Term.valueOf(uri);
            strings[i] = new String(sorted[i].local, UTF_8);
        }
        if (size > 1) {
            assertEquals(1, stream(sorted).map(t -> t.flaggedDictId).distinct().count());
            assertEquals("<http://www.example.org/ns#", RopeDict.get(sorted[0].flaggedDictId).toString());
        }

        for (int i = 0; i < size; i++) {
            byte[] local = String.format("%s%03d>", localPrefix, i).getBytes(UTF_8);
            String ctx = "localPrefix=" + localPrefix + ", i=" + i;
            assertEquals(i, binarySearchLocal(sorted, 0, size, local), ctx);
            assertEquals(i, binarySearchLocal(sorted, 0, i+1, local), ctx);
            assertEquals(i, binarySearchLocal(sorted, Math.max(0, i-1), i+1, local), ctx);

            byte[] after = String.format("%s%03d~>", localPrefix, i).getBytes(UTF_8);
            assertEquals(Arrays.binarySearch(strings, new String(after)),
                         binarySearchLocal(sorted, 0, size, after), ctx);
            assertEquals(Arrays.binarySearch(strings, 0, i+1, new String(after)),
                         binarySearchLocal(sorted, 0, i+1, after), ctx);
        }
    }


}