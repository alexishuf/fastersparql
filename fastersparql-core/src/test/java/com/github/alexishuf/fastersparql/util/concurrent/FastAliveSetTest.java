package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FastAliveSetTest {
    private record Obj(int i) { }

    private static final Obj[] O = new Obj[10_000];
    static {
        for (int i = 0; i < O.length; i++) O[i] = new Obj(i);
    }

    private void check(FastAliveSet<Obj> set, int... ids) {
        Obj[] objs = new Obj[ids.length];
        for (int i = 0; i < ids.length; i++)
            objs[i] = O[ids[i]];
        check(set, objs);
    }

    private void check(FastAliveSet<Obj> set, Obj[] objs) {
        Obj[] unseen = Arrays.copyOf(objs, objs.length);
        List<Obj> unexpected = new ArrayList<>();
        set.destruct(o -> {
            boolean seen = false;
            for (int i = 0; !seen && i < unseen.length; i++) {
                if (unseen[i] == o) {
                    unseen[i] = null;
                    seen      = true;
                }
            }
            if (!seen)
                unexpected.add(o);
        });
        assertEquals(List.of(), unexpected, "unexpected items present in set");
        int missing = 0;
        for (Obj o : unseen) {
            if (o != null) ++missing;
        }
        assertEquals(0, missing, "items missing from set");
    }


    @Test void simpleTest() {
        FastAliveSet<Obj> set = new FastAliveSet<>(6);
        set.add(O[0]);
        set.remove(O[0]);
        check(set);

        set.add(O[0]);
        set.add(O[1]);
        check(set, 0, 1);
        check(set);

        set.add(O[3]);
        set.add(O[2]);
        set.add(O[1]);
        set.add(O[0]);
        set.remove(O[1]);
        set.add(O[4]);
        set.add(O[5]);
        check(set, 3, 2, 0, 4, 5);

        set.add(O[0]);
        set.add(O[0]);
        check(set, 0, 0);


        set.add(O[0]);
        set.add(O[1]);
        set.add(O[2]);
        set.add(O[3]);
        set.add(O[4]);
        set.add(O[5]);
        check(set, 0, 1, 2, 3, 4, 5);

        set.add(O[0]);
        set.add(O[1]);
        set.add(O[2]);
        set.add(O[3]);
        set.add(O[4]);
        set.add(O[5]);
        set.remove(O[1]);
        set.add(O[6]);
        set.add(O[7]);
        check(set, 0, 2, 3, 4, 5, 6, 7);
    }

    @ValueSource(ints = {1, 3, 4, 5, 64, 256, 1_000})
    @ParameterizedTest void testBig(int buckets) {
        FastAliveSet<Obj> set = new FastAliveSet<>(buckets);
        for (Obj o : O)
            set.add(o);
        check(set, O);
    }

    @ValueSource(ints = {1, 6, 1024})
    @ParameterizedTest void testConcurrent(int buckets) throws Exception {
        int threads = Math.min(4, Runtime.getRuntime().availableProcessors()*2);
        int chunk = O.length/threads;
        FastAliveSet<Obj> set = new FastAliveSet<>(buckets);
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            tasks.repeat(threads, thread -> {
                int begin = thread*chunk, end = thread == threads-1 ? O.length : begin+chunk;
                for (int rep = 0; rep < 20; rep++) {
                    for (int i = begin; i < end; i++) {
                        set.add(O[i]);
                        set.remove(O[i]);
                    }
                }
                for (int rep = 0; rep < 20; rep++) {
                    for (int i = begin; i < end; i++) set.remove(O[i]);
                    for (int i = begin; i < end; i++) set.    add(O[i]);
                }
            });
        }
        check(set, O);
    }


}