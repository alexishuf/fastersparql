package com.github.alexishuf.fastersparql.client.util.bind;


import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RopeArrayMapTest {
    public static Stream<Arguments> testAddSorted() {
        return Stream.of(0, 1, 2, 3, 4, 10, 15, 16, 17, 22, 32, 127, 128, 129, 255, 256, 257)
                     .map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testAddSorted(int n) {
        RopeArrayMap map = new RopeArrayMap();
        for (int i = 0; i < n; i++) {
            ByteRope k = new ByteRope("k"+i);
            Rope v = Rope.of("v"+i), x = Rope.of("x"+i);
            assertNull(map.get(k), "for k="+k);
            assertEquals(i, map.size());

            map.put(k, v);
            assertEquals(i+1, map.size());
            assertEquals(v, map.get(k));

            map.put(k, x);
            assertEquals(i+1, map.size());
            assertEquals(x, map.get(k));

            map.put(k, v);
            assertEquals(i+1, map.size());
            assertEquals(v, map.get(k));

            for (int j = 0; j < i; j++) {
                String ctx = "i=" + i + ", j=" + j;
                ByteRope ex = Rope.of('v', j), key = Rope.of('k', j);
                assertEquals(ex, map.get(key), ctx);
                assertEquals(ex, map.get(Rope.of(" k",j), 1,1+key.len));
                assertEquals(ex, map.get(Rope.of("~k",j," "), 1,1+key.len));
            }
        }

    }

    public static Stream<Arguments> testAdd() {
        List<List<Integer>> lists = new ArrayList<>(List.of(
                List.of(2, 1),
                List.of(7, 3, 4, 6, 1),
                List.of(1, 2, 9, 5, 7, 3, 8)
        ));
        for (Integer center : List.of(16, 16*4, 16*8)) {
            for (Integer size : List.of(center - 1, center, center + 1)) {
                List<Integer> list = new ArrayList<>(IntStream.range(0, size).boxed().toList());
                Collections.reverse(list);
                lists.add(list);
            }
        }
        return lists.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testAdd(List<Integer> ids) {
        RopeArrayMap map = new RopeArrayMap();
        for (int i = 0; i < ids.size(); i++) {
            ByteRope k = new ByteRope("k" + ids.get(i));
            Rope v = Rope.of("v" + ids.get(i));
            assertEquals(i, map.size());
            assertNull(map.get(k));

            map.put(k, v);
            assertEquals(v, map.get(k));
            assertEquals(i+1, map.size());
        }
        for (Integer id : ids) {
            Rope k = Rope.of("k" + id), v = Rope.of("v" + id);
            assertEquals(v, map.get(k));
            assertEquals(v, map.get(Rope.of(' ', k), 1, 1+k.len));
            assertEquals(v, map.get(Rope.of('~', k, ' '), 1, 1+k.len));
        }
    }
}