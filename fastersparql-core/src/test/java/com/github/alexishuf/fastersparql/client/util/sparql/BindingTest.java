package com.github.alexishuf.fastersparql.client.util.sparql;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.binding.ListBinding;
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
                BindingTest::fillListByName
        );

        final class P {
            final List<String> vars;
            final List<String> values;

            public P(List<String> vars, List<String> values) {
                this.vars = vars;
                this.values = values;
            }

            List<String> vars() {return vars;}
            List<String> values() {return values;}
        }

        return Stream.of(
                new P(List.of(),              emptyList()),
                new P(List.of("x"),           singletonList("\"23\"")),
                new P(List.of("x"),           singletonList(null)),
                new P(List.of("x", "y"),      asList("<a>", "<b>")),
                new P(List.of("x", "y"),      asList("<a>", null)),
                new P(List.of("x", "y"),      asList(null, "<b>")),
                new P(List.of("x", "y", "z"), asList("<a>", "<b>", "<c>")),
                new P(List.of("z", "x", "y"), asList("<c>", "<a>", "<b>")),
                new P(List.of("z", "x", "y"), asList("<c>", null, "<b>"))
        ).flatMap(p -> factories.stream().map(f -> arguments(f, p.vars, p.values)));
    }

    private static Binding wrapArray(List<String> varsList, List<? extends CharSequence> values) {
        Vars vars = Vars.fromSet(varsList);
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

    private static LinkedHashMap<String, @Nullable String> toMap(List<String> vars,
                                                                 List<? extends CharSequence> values) {
        LinkedHashMap<String, @Nullable String> map = new LinkedHashMap<>();
        for (int i = 0; i < vars.size(); i++) {
            CharSequence cs = values.get(i);
            map.put(vars.get(i), cs == null ? null : cs.toString());
        }
        return map;
    }

    private static Binding copyArray(List<String> vars, List<? extends CharSequence> values) {
        return ArrayBinding.copy(toMap(vars, values));
    }

    private static Binding fillArrayByIndex(List<String> vars,
                                            List<? extends CharSequence> values) {
        ArrayBinding binding = new ArrayBinding(Vars.fromSet(vars));
        for (int i = 0; i < values.size(); i++)
            binding.set(i, values.get(i) == null ? null : values.get(i).toString());
        return binding;
    }

    private static Binding fillArrayByName(List<String> vars,
                                            List<? extends CharSequence> values) {
        ArrayBinding binding = new ArrayBinding(Vars.fromSet(vars));
        for (int i = 0; i < values.size(); i++)
            binding.set(vars.get(i), values.get(i) == null ? null : values.get(i).toString());
        return binding;
    }

    private static Binding wrapList(List<String> vars, List<? extends CharSequence> values) {
        return ListBinding.wrap(Vars.fromSet(vars), values);
    }

    private static Binding copyList(List<String> vars, List<? extends CharSequence> values) {
        return ListBinding.copy(toMap(vars, values));
    }

    private static Binding fillListByIndex(List<String> vars,
                                            List<? extends CharSequence> values) {
        ListBinding binding = new ListBinding(Vars.from(vars));
        for (int i = 0; i < values.size(); i++)
            binding.set(i, values.get(i) == null ? null : values.get(i).toString());
        return binding;
    }

    private static Binding fillListByName(List<String> vars,
                                           List<? extends CharSequence> values) {
        ListBinding binding = new ListBinding(Vars.from(vars));
        for (int i = 0; i < values.size(); i++)
            binding.set(vars.get(i), values.get(i) == null ? null : values.get(i).toString());
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
        assertEquals(vars, new ArrayList<>(binding.vars));

        List<String> idxValues = new ArrayList<>();
        for (int i = 0; i < binding.size(); i++) idxValues.add(binding.get(i));
        assertEquals(values, idxValues);

        List<String> varValues = new ArrayList<>();
        for (String name : vars) varValues.add(binding.get(name));
        assertEquals(values, varValues);

        assertNull(binding.get("non-existing"));
        assertNull(binding.get("non existing and invalid"));

        assertThrows(IndexOutOfBoundsException.class, () -> binding.get(binding.size()));
        assertThrows(IndexOutOfBoundsException.class, () -> binding.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> binding.get(binding.size()+1));
        assertThrows(IndexOutOfBoundsException.class, () -> binding.get(-2));
    }

}