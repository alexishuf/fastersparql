package com.github.alexishuf.fastersparql.client.util.sparql;

import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BindingTest {
    
    @SuppressWarnings("unused") public static Stream<Arguments> testAccess() {
        List<BiFunction<List<String>, List<? extends CharSequence>, Binding>> factories = asList(
                BindingTest::wrapArray,
                BindingTest::copyArray,
                BindingTest::fillArrayByIndex,
                BindingTest::fillArrayByName,

                BindingTest::wrapList,
                BindingTest::copyList,
                BindingTest::fillListByIndex,
                BindingTest::fillListByName,

                BindingTest::wrapMap1,
                BindingTest::wrapMap2,
                BindingTest::wrapMap3,
                BindingTest::fillMapByIndex,
                BindingTest::fillMapByName
        );

        @Value @Accessors(fluent = true)
        class P {
            List<String> vars;
            List<String> values;
        }

        return Stream.of(
                new P(emptyList(), emptyList()),
                new P(singletonList("x"), singletonList("\"23\"")),
                new P(singletonList("x"), singletonList(null)),
                new P(asList("x", "y"), asList("<a>", "<b>")),
                new P(asList("x", "y"), asList("<a>", null)),
                new P(asList("x", "y"), asList(null, "<b>")),
                new P(asList("x", "y", "z"), asList("<a>", "<b>", "<c>")),
                new P(asList("z", "x", "y"), asList("<c>", "<a>", "<b>")),
                new P(asList("z", "x", "y"), asList("<c>", null, "<b>"))
        ).flatMap(p -> factories.stream().map(f -> arguments(f, p.vars, p.values)));
    }

    private static Binding wrapArray(List<String> varsList, List<? extends CharSequence> values) {
        String[] vars = varsList.toArray(new String[0]);
        if (values.stream().filter(Objects::nonNull).allMatch(String.class::isInstance)) {
            String[] stringValues = values.stream().map(s -> s == null ? null : s.toString())
                                          .toArray(String[]::new);
            return ArrayBinding.wrap(vars, stringValues);
        } else {
            CharSequence[] csValues = values.stream().map(s -> (CharSequence)s)
                    .toArray(CharSequence[]::new);
            return ArrayBinding.wrap(vars, csValues);
        }
    }

    private static LinkedHashMap<String, @Nullable String> toMap(List<String> varsList,
                                                                 List<? extends CharSequence> values) {
        LinkedHashMap<String, @Nullable String> map = new LinkedHashMap<>();
        for (int i = 0; i < varsList.size(); i++) {
            CharSequence cs = values.get(i);
            map.put(varsList.get(i), cs == null ? null : cs.toString());
        }
        return map;
    }

    private static Binding copyArray(List<String> varsList, List<? extends CharSequence> values) {
        return ArrayBinding.copy(toMap(varsList, values));
    }

    private static Binding fillArrayByIndex(List<String> varsList,
                                            List<? extends CharSequence> values) {
        ArrayBinding binding = new ArrayBinding(varsList);
        for (int i = 0; i < values.size(); i++)
            binding.set(i, values.get(i) == null ? null : values.get(i).toString());
        return binding;
    }

    private static Binding fillArrayByName(List<String> varsList,
                                            List<? extends CharSequence> values) {
        ArrayBinding binding = new ArrayBinding(varsList);
        for (int i = 0; i < values.size(); i++)
            binding.set(varsList.get(i), values.get(i) == null ? null : values.get(i).toString());
        return binding;
    }

    private static Binding wrapList(List<String> varsList, List<? extends CharSequence> values) {
        return ListBinding.wrap(varsList, values);
    }

    private static Binding copyList(List<String> varsList, List<? extends CharSequence> values) {
        return ListBinding.copy(toMap(varsList, values));
    }

    private static Binding fillListByIndex(List<String> varsList,
                                            List<? extends CharSequence> values) {
        ListBinding binding = new ListBinding(varsList);
        for (int i = 0; i < values.size(); i++)
            binding.set(i, values.get(i) == null ? null : values.get(i).toString());
        return binding;
    }

    private static Binding fillListByName(List<String> varsList,
                                           List<? extends CharSequence> values) {
        ListBinding binding = new ListBinding(varsList);
        for (int i = 0; i < values.size(); i++)
            binding.set(varsList.get(i), values.get(i) == null ? null : values.get(i).toString());
        return binding;
    }

    private static Binding wrapMap1(List<String> varsList, List<? extends CharSequence> values) {
        return MapBinding.wrap(toMap(varsList, values));
    }

    private static Binding wrapMap2(List<String> varsList, List<? extends CharSequence> values) {
        return MapBinding.wrap(varsList, toMap(varsList, values));
    }

    private static Binding wrapMap3(List<String> varsList, List<? extends CharSequence> values) {
        return MapBinding.wrap(varsList.toArray(new String[0]), toMap(varsList, values));
    }

    private static Binding fillMapByIndex(List<String> varsList,
                                           List<? extends CharSequence> values) {
        LinkedHashMap<String, @Nullable String> map = new LinkedHashMap<>();
        for (String var : varsList)
            map.put(var, null);
        MapBinding binding = new MapBinding(varsList.toArray(new String[0])).values(map);
        for (int i = 0; i < values.size(); i++)
            binding.set(i, values.get(i) == null ? null : values.get(i).toString());
        return binding;
    }

    private static Binding fillMapByName(List<String> varsList,
                                          List<? extends CharSequence> values) {
        LinkedHashMap<String, @Nullable String> map = new LinkedHashMap<>();
        for (String var : varsList)
            map.put(var, null);
        MapBinding binding = new MapBinding(varsList.toArray(new String[0])).values(map);
        for (int i = 0; i < values.size(); i++)
            binding.set(varsList.get(i), values.get(i) == null ? null : values.get(i).toString());
        return binding;
    }

    @ParameterizedTest @MethodSource
    void testAccess(BiFunction<List<String>, List<? extends CharSequence>, Binding> factory,
                    List<String> vars, List<String> values) {
        doTestAccess(factory.apply(vars, values), vars, values);
        List<CharSequence> csValues = new ArrayList<>();
        for (String value : values)
            csValues.add(value == null ? null : new StringBuilder(value));
        doTestAccess(factory.apply(vars, csValues), vars, values);
    }

    void doTestAccess(Binding binding, List<String> vars, List<String> values) {
        List<String> acVars = new ArrayList<>();
        for (int i = 0; i < binding.size(); i++) acVars.add(binding.var(i));
        assertEquals(vars, acVars);

        List<String> seqValues = new ArrayList<>();
        for (int i = 0; i < binding.size(); i++) seqValues.add(binding.get(i));
        assertEquals(values, seqValues);

        List<String> acValues = new ArrayList<>();
        for (String name : vars) acValues.add(binding.get(name));
        assertEquals(values, acValues);

        for (String var : vars) {
            assertTrue(binding.contains(var), var+" is missing");
            assertFalse(binding.contains(var+"extra"), var+"extra found!");
        }
        assertFalse(binding.contains("outside"));

        for (int i = 0; i < vars.size(); i++) {
            assertEquals(i, binding.indexOf(vars.get(i)));
            assertEquals(-1, binding.indexOf(vars.get(i)+"extra"));
        }
        assertEquals(-1, binding.indexOf("outside"));

        assertNull(binding.get("non-existing"));
        assertNull(binding.get("non existing and invalid"));

        assertThrows(IndexOutOfBoundsException.class, () -> binding.get(binding.size()));
        assertThrows(IndexOutOfBoundsException.class, () -> binding.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> binding.get(binding.size()+1));
        assertThrows(IndexOutOfBoundsException.class, () -> binding.get(-2));

        assertThrows(IndexOutOfBoundsException.class, () -> binding.var(binding.size()));
        assertThrows(IndexOutOfBoundsException.class, () -> binding.var(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> binding.var(binding.size()+1));
        assertThrows(IndexOutOfBoundsException.class, () -> binding.var(-2));
    }

}