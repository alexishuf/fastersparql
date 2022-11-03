package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Stream;

import static java.lang.Integer.MAX_VALUE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings("unused")
class VarsTest {
    private static List<String> list(int begin, int end) {
        return range(begin, end).mapToObj(Integer::toString).collect(toList());
    }

    private static List<String> revList(int begin, int end) {
        var list = new ArrayList<>(range(begin, end).mapToObj(Integer::toString).toList());
        Collections.reverse(list);
        return list;
    }

    private static abstract class Factory {
        public abstract Vars create(int size, int slack);
        @Override public String toString() { return getClass().getSimpleName(); }
    }


    private static class WrapFactory extends Factory {
        @Override public Vars create(int size, int slack) {
            String[] array = new String[size+slack];
            for (int i = 0; i < size; i++)
                array[i] = Integer.toString(i);
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
                vars.add(Integer.toString(i));
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
        try (var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
            for (Method m : getClass().getMethods()) {
                String name = m.getName();
                if (!name.matches("test.+") || name.equals("testAll")) continue;
                ++methods;
                for (D d : name.equals("tesQuery") ? data : mutableData)
                    tasks.add(() -> m.invoke(this, d.factory, d.size, d.slack));
            }
        }
        assertEquals(14, methods); //will break with new methods: thats intentional
    }

//    @ParameterizedTest @MethodSource("data")
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testQuery(Factory factory, int size, int slack) {
        Vars vars = factory.create(size, slack);
        assertEquals(size, vars.size());
        assertEquals(size == 0, vars.isEmpty());
        assertEquals(size > 0, vars.iterator().hasNext());

        //get()-based iteration
        for (int i = 0; i < size; i++)
            assertEquals(Integer.toString(i), vars.get(i));
        assertThrows(IndexOutOfBoundsException.class, () -> vars.get(size));
        assertThrows(IndexOutOfBoundsException.class, () -> vars.get(size+1));
        assertThrows(IndexOutOfBoundsException.class, () -> vars.get(-1));

        //iterator()/stream() iteration
        var expected = list(0, size);
        assertEquals(expected, new ArrayList<>(vars));
        assertEquals(expected, vars.stream().toList());

        // indexOf
        for (int i = 0; i < size; i++) {
            assertEquals(i, vars.indexOf(Integer.toString(i)));
            assertEquals(i, vars.lastIndexOf(Integer.toString(i)));
        }

        // negative indexOf
        assertEquals(-1, vars.indexOf(""));
        for (int i = size; i < size + slack + 1; i++)
            assertEquals(-1, vars.indexOf(Integer.toString(i)));

        // contains
        for (int i = 0; i < size; i++)
            assertTrue(vars.contains(Integer.toString(i)));

        // negative contains
        assertFalse(vars.contains(""));
        for (int i = size; i < size + slack + 1; i++)
            assertFalse(vars.contains(Integer.toString(i)));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    public void testToString(Factory factory, int size, int slack) {
        assertEquals(list(0, size).toString(),
                     factory.create(size, slack).toString());
    }

    @SuppressWarnings("SimplifiableAssertion")
//    @ParameterizedTest @MethodSource("mutableData")
    public void testEqualsAndHashCode(Factory factory, int size, int slack) {
        Vars a = factory.create(size, slack), b = factory.create(size, slack);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a.toString(), b.toString());
        assertTrue(a.equals(b));
        assertTrue(b.equals(a));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    public void testAddNoEffect(Factory factory, int size, int slack) {
        Vars vars = factory.create(size, slack);

        assertFalse(vars.addAll(list(0, size)));
        assertEquals(list(0, size), new ArrayList<>(vars));

        assertFalse(vars.addAll(revList(0, size)));
        assertEquals(list(0, size), new ArrayList<>(vars));

        assertFalse(vars.addAll(new HashSet<>(list(0, size))));
        assertEquals(list(0, size), new ArrayList<>(vars));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    public void testAdd(Factory factory, int size, int slack) {
        Vars v = factory.create(size, slack);
        assertTrue(list(size, size * 2 + 1).stream().allMatch(v::add));
        assertEquals(list(0, size * 2 + 1), new ArrayList<>(v));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    public void testAddReversedDoubled(Factory factory, int size, int slack) {
        Vars v = factory.create(size, slack);
        for (int i = size*2; i >= size; i--) {
            assertTrue(v.add(Integer.toString(i)));
            assertFalse(v.add(Integer.toString(i)));
        }
        var expected = list(0, size);
        expected.addAll(revList(size, size*2+1));
        assertEquals(expected, new ArrayList<>(v));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    public void testAddAllNoEffect(Factory factory, int size, int slack) {
        Vars v = factory.create(size, slack);
        assertFalse(v.addAll(list(0, size)));
        assertFalse(v.addAll(revList(0, size)));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    public void testAddAll(Factory factory, int size, int slack) {
        Vars v = factory.create(size, slack);
        var slackList = list(size, size + slack);
        assertEquals(slack > 0, v.addAll(slackList));
        assertEquals(list(0, size+slack), new ArrayList<>(v));

        var suffix = list(0, 2*size+slack+1);
        assertTrue(v.addAll(suffix));
        assertEquals(list(0, 2*size+slack+1), new ArrayList<>(v));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    public void testNovelItemsList(Factory factory, int size, int slack) {
        Vars v = factory.create(size, slack);
        assertEquals(0, v.novelItems(list(0, size), MAX_VALUE, 0));
        assertEquals(0, v.novelItems(list(0, size), 1, 0));

        assertEquals(0, v.novelItems(revList(0, size), MAX_VALUE, 0));
        assertEquals(0, v.novelItems(revList(0, size), 1, 0));

        assertEquals(slack+1, v.novelItems(list(size, size+slack+1), MAX_VALUE, 0));
        assertEquals(1, v.novelItems(list(size, size+slack+1), MAX_VALUE, slack));
        assertEquals(1, v.novelItems(revList(size, size+slack+1), MAX_VALUE, slack));

        assertEquals(1, v.novelItems(list(-1, 0), MAX_VALUE, 0));
        assertEquals(2, v.novelItems(list(-2, 0), MAX_VALUE, 0));

        assertEquals(2, v.novelItems(list(size, size+2), 2, 0));
        assertEquals(2, v.novelItems(revList(size, size+2), 2, 0));
        assertEquals(1, v.novelItems(list(size, size+2), 1, 0));
        assertEquals(1, v.novelItems(list(size, size+2), 1, 1));
        assertEquals(1, v.novelItems(list(size, size+2), 1, 1));


        List<String> list = revList(size - 1, size + 1);
        assertEquals(v.isEmpty() ? 2 : 1, v.novelItems(list, MAX_VALUE, 0));
        assertEquals(v.isEmpty() ? 1 : 0, v.novelItems(list, MAX_VALUE, 1));
        assertEquals(v.isEmpty() ? 1 : 0, v.novelItems(list, 1, 1));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    public void testNovelItemsSet(Factory factory, int size, int slack) {
        Vars v = factory.create(size, slack);

        assertEquals(1, v.novelItems(new HashSet<>(list(size, size+1)), MAX_VALUE));
        assertEquals(v.isEmpty() ? 1 : 0, v.novelItems(new HashSet<>(list(size-1, size)), MAX_VALUE));
        assertEquals(slack+1, v.novelItems(new HashSet<>(list(size, size+slack+1)), MAX_VALUE));
        assertEquals(1, v.novelItems(new HashSet<>(list(size, size+slack+1)), 1));
        assertEquals(1, v.novelItems(new HashSet<>(list(size, size+slack+2)), 1));

        assertEquals(2, v.novelItems(Set.of("-2", "-1"), MAX_VALUE));
        assertEquals(1, v.novelItems(Set.of("-2", "-1"), 1));
    }

//    @ParameterizedTest @MethodSource("mutableData")
    public void testPlus(Factory factory, int size, int slack) {
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
    public void testMinus(Factory factory, int size, int slack) {
        Vars left = factory.create(size, slack);

        // remove all
        assertEquals(List.of(), new ArrayList<>(left.minus(Vars.from(list(0, size)))));
        assertEquals(list(0, size), new ArrayList<>(left), "left mutated");

        // remove one item
        for (int i = 0; i < size; i++) {
            Vars right = Vars.of(Integer.toString(i));
            List<String> expected = list(0, size);
            expected.remove(Integer.toString(i));
            assertEquals(expected, new ArrayList<>(left.minus(right)));
            assertEquals(list(0, size), new ArrayList<>(left), "left mutated");
        }

        //remove nothing
        assertSame(left, left.minus(Vars.of("-1", Integer.toString(size))));
        assertEquals(list(0, size), left, "left mutated");
    }

//    @ParameterizedTest @MethodSource("mutableData")
    public void testIntersects(Factory factory, int size, int slack) {
        Vars left = factory.create(size, slack);
        if (left.isEmpty()) {
            assertFalse(left.intersects(List.of()));
            assertFalse(left.intersects(List.of("0")));
            assertFalse(left.intersects(List.of("-1")));
        } else {
            //noinspection CollectionAddedToSelf
            assertTrue(left.intersects(left));
            assertTrue(left.intersects(list(0, size)));
            assertTrue(left.intersects(Vars.from(list(0, size))));

            assertFalse(left.intersects(List.of("-1")));
            assertFalse(left.intersects(Vars.of("-1")));
            assertFalse(left.intersects(List.of(Integer.toString(size))));
            assertFalse(left.intersects(Vars.of(Integer.toString(size))));

            for (int i = 0; i < size; i++) {
                assertTrue(left.intersects(List.of(Integer.toString(i))));
                assertTrue(left.intersects(Vars.of(Integer.toString(i))));
            }
        }
    }

//    @ParameterizedTest @MethodSource("mutableData")
    public void testIntersection(Factory factory, int size, int slack) {
        Vars left = factory.create(size, slack);

        assertSame(left.isEmpty() ? Vars.EMPTY : left,   left.intersection(list(0, size)));
        assertEquals(left.isEmpty() ? Vars.EMPTY : left, left.intersection(revList(0, size)));

        assertSame(Vars.EMPTY, left.intersection(Vars.EMPTY));
        assertSame(Vars.EMPTY, left.intersection(new Vars.Mutable(10)));
        assertSame(Vars.EMPTY, left.intersection(Vars.of("-1", ""+size)));

        for (int i = 0; i < size; i++) {
            Vars right = Vars.of(Integer.toString(i));
            assertEquals(new ArrayList<>(right), new ArrayList<>(left.intersection(right)));
            assertSame(size == 1 ? left : right, left.intersection(right));
        }

        for (int i = 0; i < size; i++) {
            Vars right = Vars.of(""+i, "-1");
            assertEquals(List.of(""+i), new ArrayList<>(left.intersection(right)));
        }

        for (int i = 0; i < size-1; i++) {
            Vars right = Vars.of(""+i, Integer.toString(i+1));
            assertEquals(List.of(""+i, Integer.toString(i+1)),
                         new ArrayList<>(left.intersection(right)));
        }
    }
}