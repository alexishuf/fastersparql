package com.github.alexishuf.fastersparql.model;

import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.Rope.ropeList;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings("unused")
class VarsTest {
    private static List<SegmentRope> list(int begin, int end) {
        return range(begin, end).mapToObj(SegmentRope::of).collect(toList());
    }

    private static List<SegmentRope> revList(int begin, int end) {
        var list = new ArrayList<>(range(begin, end).mapToObj(SegmentRope::of).toList());
        Collections.reverse(list);
        return list;
    }

    static abstract class Factory {
        public abstract Vars create(int size, int slack);
        @Override public String toString() { return getClass().getSimpleName(); }
    }


    private static class WrapFactory extends Factory {
        @Override public Vars create(int size, int slack) {
            SegmentRope[] array = new SegmentRope[size+slack];
            for (int i = 0; i < size; i++)
                array[i] = SegmentRope.of(i);
            return Vars.wrapSet(array, size);
        }
    }

    private static class FromSetFactory extends Factory {
        @Override public Vars create(int size, int slack) {
            var set = new LinkedHashSet<String>();
            for (int i = 0; i < size; i++)
                set.add(Integer.toString(i));
            return slack == 0 ? Vars.fromSet(set) : Vars.fromSet(set, size+slack);
        }
    }

    private static class FromCollectionFactory extends Factory {
        @Override public Vars create(int size, int slack) {
            var list = range(0, size).mapToObj(Integer::toString).toList();
            return slack == 0 ? Vars.from(list) : Vars.from(list, size+slack);
        }
    }

    private static class AddFactory extends Factory {
        @Override public Vars create(int size, int slack) {
            Vars.Mutable vars = new Vars.Mutable(size+slack);
            for (int i = 0; i < size; i++)
                vars.add(SegmentRope.of(i));
            return vars;
        }
    }

    private static class AddAllFactory extends Factory {
        @Override public Vars create(int size, int slack) {
            Vars.Mutable vars = new Vars.Mutable(0);
            vars.addAll(list(0, size));
            return vars;
        }
    }


    static Stream<Arguments> mutableData() {
        return data(List.of(
                new FromCollectionFactory(),
                new FromSetFactory(),
                new AddFactory(),
                new AddAllFactory()
        ));
    }

    static Stream<Arguments> data() {
        List<Factory> factories = List.of(
                new WrapFactory(),
                new FromCollectionFactory(),
                new FromSetFactory(),
                new AddFactory(),
                new AddAllFactory()
        );
        return data(factories);
    }

    static Stream<Arguments> data(List<Factory> factories) {
        List<Arguments> list = new ArrayList<>();
        for (Factory fac : factories) {
            for (Integer size : List.of(0, 1, 2, 3, 9, 10, 11, 15, 16, 64, 128)) {
                for (Integer slack : List.of(0, 1, 10))
                    list.add(arguments(fac, size, slack));
            }
        }
        return list.stream();
    }

    @Test void testAll() throws Exception {
        record D(Factory factory, int size, int slack) {
            public D(Arguments a) { this((Factory) a.get()[0], (Integer)a.get()[1], (Integer) a.get()[2]); }
        }
        List<D> data        = data().map(D::new).toList();
        List<D> mutableData = mutableData().map(D::new).toList();
        int methods = 0;
        try (var tasks = TestTaskSet.virtualTaskSet(getClass().getSimpleName())) {
            for (Method m : getClass().getDeclaredMethods()) {
                String name = m.getName();
                if (!name.matches("test.+") || name.equals("testAll")) continue;
                ++methods;
                for (D d : name.equals("tesQuery") ? data : mutableData)
                    tasks.add(() -> m.invoke(this, d.factory, d.size, d.slack));
            }
        }
        assertEquals(12, methods); //will break with new methods: thats intentional
    }

    @ParameterizedTest @MethodSource("data")
    void testQuery(Factory factory, int size, int slack) {
        Vars vars = factory.create(size, slack);
        assertEquals(size, vars.size());
        assertEquals(size == 0, vars.isEmpty());
        assertEquals(size > 0, vars.iterator().hasNext());

        //get()-based iteration
        for (int i = 0; i < size; i++)
            assertEquals(Rope.of(i), vars.get(i));
        assertThrows(IndexOutOfBoundsException.class, () -> vars.get(size));
        assertThrows(IndexOutOfBoundsException.class, () -> vars.get(size+1));
        assertThrows(IndexOutOfBoundsException.class, () -> vars.get(-1));

        //iterator()/stream() iteration
        var expected = list(0, size);
        assertEquals(expected, new ArrayList<>(vars));
        assertEquals(expected, vars.stream().toList());

        // indexOf
        for (int i = 0; i < size; i++) {
            var iStr = SegmentRope.of(i);
            assertEquals(i, vars.indexOf(iStr), "i="+i);
            assertEquals(i, vars.indexOf(Term.valueOf("?"+i)), "i="+i);
            assertEquals(i, vars.indexOf(Term.valueOf("$"+i)), "i="+i);
            assertEquals(i, vars.lastIndexOf(iStr), "i="+i);
        }

        // negative indexOf
        assertEquals(-1, vars.indexOf(ByteRope.EMPTY));
        for (int i = size; i < size + slack + 1; i++)
            assertEquals(-1, vars.indexOf(SegmentRope.of(i)));

        // contains
        for (int i = 0; i < size; i++)
            assertTrue(vars.contains(SegmentRope.of(i)));

        // negative contains
        assertFalse(vars.contains(ByteRope.EMPTY));
        for (int i = size; i < size + slack + 1; i++)
            assertFalse(vars.contains(SegmentRope.of(i)));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    void testToString(Factory factory, int size, int slack) {
        assertEquals(list(0, size).toString(),
                     factory.create(size, slack).toString());
    }

    @SuppressWarnings("SimplifiableAssertion")
//    @ParameterizedTest @MethodSource("mutableData")
    void testEqualsAndHashCode(Factory factory, int size, int slack) {
        Vars a = factory.create(size, slack), b = factory.create(size, slack);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a.toString(), b.toString());
        assertTrue(a.equals(b));
        assertTrue(b.equals(a));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    void testAddNoEffect(Factory factory, int size, int slack) {
        Vars vars = factory.create(size, slack);

        assertFalse(vars.addAll(list(0, size)));
        assertEquals(list(0, size), new ArrayList<>(vars));

        assertFalse(vars.addAll(revList(0, size)));
        assertEquals(list(0, size), new ArrayList<>(vars));

        assertFalse(vars.addAll(new HashSet<>(list(0, size))));
        assertEquals(list(0, size), new ArrayList<>(vars));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    void testAdd(Factory factory, int size, int slack) {
        Vars v = factory.create(size, slack);
        assertTrue(list(size, size * 2 + 1).stream().allMatch(v::add));
        assertEquals(list(0, size * 2 + 1), new ArrayList<>(v));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    void testAddReversedDoubled(Factory factory, int size, int slack) {
        Vars v = factory.create(size, slack);
        for (int i = size*2; i >= size; i--) {
            assertTrue(v.add(SegmentRope.of(i)));
            assertFalse(v.add(SegmentRope.of(i)));
        }
        var expected = list(0, size);
        expected.addAll(revList(size, size*2+1));
        assertEquals(expected, new ArrayList<>(v));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    void testAddAllNoEffect(Factory factory, int size, int slack) {
        Vars v = factory.create(size, slack);
        assertFalse(v.addAll(list(0, size)));
        assertFalse(v.addAll(revList(0, size)));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    void testAddAll(Factory factory, int size, int slack) {
        Vars v = factory.create(size, slack);
        var slackList = list(size, size + slack);
        assertEquals(slack > 0, v.addAll(slackList));
        assertEquals(list(0, size+slack), new ArrayList<>(v));

        var suffix = list(0, 2*size+slack+1);
        assertTrue(v.addAll(suffix));
        assertEquals(list(0, 2*size+slack+1), new ArrayList<>(v));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    void testPlus(Factory factory, int size, int slack) {
        Vars left = factory.create(size, slack);

        for (Integer n : List.of(0, slack, slack + 1)) {
            Vars plus = left.union(Vars.from(list(0, size + n)));
            assertEquals(list(0, size+n), new ArrayList<>(plus));
            assertEquals(list(0, size), left, "left mutated");
            if (plus.size == left.size)
                assertSame(left, plus);
        }
    }

//    @ParameterizedTest @MethodSource("mutableData")
    void testMinus(Factory factory, int size, int slack) {
        Vars left = factory.create(size, slack);

        // remove all
        assertEquals(List.of(), new ArrayList<>(left.minus(Vars.from(list(0, size)))));
        assertEquals(list(0, size), new ArrayList<>(left), "left mutated");

        // remove one item
        for (int i = 0; i < size; i++) {
            Vars right = Vars.of(Rope.of(i));
            List<SegmentRope> expected = list(0, size);
            expected.remove(SegmentRope.of(i));
            assertEquals(expected, new ArrayList<>(left.minus(right)));
            assertEquals(list(0, size), new ArrayList<>(left), "left mutated");
        }

        //remove nothing
        assertSame(left, left.minus(Vars.of(Rope.of("-1"), Rope.of(size))));
        assertEquals(list(0, size), left, "left mutated");
    }

//    @ParameterizedTest @MethodSource("mutableData")
    void testIntersects(Factory factory, int size, int slack) {
        Vars left = factory.create(size, slack);
        if (left.isEmpty()) {
            assertFalse(left.intersects(List.of()));
            assertFalse(left.intersects(termList("0")));
            assertFalse(left.intersects(termList("-1")));
        } else {
            //noinspection CollectionAddedToSelf
            assertTrue(left.intersects(left));
            assertTrue(left.intersects(list(0, size)));
            assertTrue(left.intersects(Vars.from(list(0, size))));

            assertFalse(left.intersects(Rope.ropeList("-1")));
            assertFalse(left.intersects(Vars.of("-1")));
            assertFalse(left.intersects(List.of(Rope.of(size))));
            assertFalse(left.intersects(Vars.of(Rope.of(size))));

            for (int i = 0; i < size; i++) {
                assertTrue(left.intersects(List.of(Rope.of(i))));
                assertTrue(left.intersects(Vars.of(Rope.of(i))));
            }
        }
    }

//    @ParameterizedTest @MethodSource("mutableData")
    void testIntersection(Factory factory, int size, int slack) {
        Vars left = factory.create(size, slack);

        assertSame(left.isEmpty() ? Vars.EMPTY : left,   left.intersection(list(0, size)));
        assertEquals(left.isEmpty() ? Vars.EMPTY : left, left.intersection(revList(0, size)));

        assertSame(Vars.EMPTY, left.intersection(Vars.EMPTY));
        assertSame(Vars.EMPTY, left.intersection(new Vars.Mutable(10)));
        assertSame(Vars.EMPTY, left.intersection(Vars.of("-1", String.valueOf(size))));

        for (int i = 0; i < size; i++) {
            Vars right = Vars.of(Rope.of(i));
            assertEquals(new ArrayList<>(right), new ArrayList<>(left.intersection(right)));
            assertSame(size == 1 ? left : right, left.intersection(right));
        }

        for (int i = 0; i < size; i++) {
            Vars right = Vars.of(String.valueOf(i), "-1");
            assertEquals(ropeList(i), new ArrayList<>(left.intersection(right)));
        }

        for (int i = 0; i < size-1; i++) {
            Vars right = Vars.of(Rope.of(i), Rope.of(i+1));
            assertEquals(List.of(Rope.of(i), Rope.of(i+1)),
                         new ArrayList<>(left.intersection(right)));
        }
    }
}