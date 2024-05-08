package com.github.alexishuf.fastersparql.client.util.bind;


import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.asFinal;
import static com.github.alexishuf.fastersparql.model.rope.Rope.asRope;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RopeArrayMapTest {
    public static Stream<Arguments> testAddSorted() {
        return Stream.of(0, 1, 2, 3, 4, 10, 15, 16, 17, 22, 32, 127, 128, 129, 255, 256, 257)
                     .map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testAddSorted(int n) {
        try (var mapGuard = new Guard<RopeArrayMap>(this)) {
            RopeArrayMap map = mapGuard.set(RopeArrayMap.create());
            for (int i = 0; i < n; i++) {
                var k = asFinal("k"+i);
                Rope v = asRope("v"+i), x = asRope("x"+i);
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
                    FinalSegmentRope ex = asFinal("v"+j), key = asFinal("k"+j);
                    assertEquals(ex, map.get(key), ctx);
                    assertEquals(ex, map.get(asFinal(" k"+j), 1,1+key.len));
                    assertEquals(ex, map.get(asFinal("~k"+j+" "), 1,1+key.len));
                }
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
        try (var mapGuard = new Guard<RopeArrayMap>(this)) {
            RopeArrayMap map = mapGuard.set(RopeArrayMap.create());
            for (int i = 0; i < ids.size(); i++) {
                var k = asFinal("k" + ids.get(i));
                Rope v = Rope.asRope("v" + ids.get(i));
                assertEquals(i, map.size());
                assertNull(map.get(k));

                map.put(k, v);
                assertEquals(v, map.get(k));
                assertEquals(i+1, map.size());
            }
            for (Integer id : ids) {
                FinalSegmentRope k = asFinal("k" + id);
                Rope v = Rope.asRope("v"+id);
                assertEquals(v, map.get(k));
                assertEquals(v, map.get(asFinal(" "+k), 1, 1+k.len));
                assertEquals(v, map.get(asFinal("~"+k+" "), 1, 1+k.len));

            }
        }
    }
}