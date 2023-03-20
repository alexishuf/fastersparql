package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BindingTest {
    private static final Term one = Term.typed(1, RopeDict.DT_integer);
    private static final Term two = Term.typed(2, RopeDict.DT_integer);
    private static final Term three = Term.typed(3, RopeDict.DT_integer);

    @BeforeAll
    static void beforeAll() {
        BatchBinding.SUPPRESS_SET_WARN = true;
    }

    @AfterAll
    static void afterAll() {
        BatchBinding.SUPPRESS_SET_WARN = false;
    }

    private static <B extends Batch<B>>
    void addCases(BatchType<B> bt, List<Arguments> argsList) {
        var b = bt.createSingleton(1);
        b.putRow(List.of(one));
        argsList.add(arguments(new BatchBinding<>(bt, Vars.of("x")).setRow(b, 0),
                               List.of(one)));

        b = bt.createSingleton(1);
        argsList.add(arguments(new BatchBinding<>(bt, Vars.of("x")).setRow(b, 0),
                              singletonList(null)));

        b = bt.createSingleton(2);
        b.putRow(Arrays.asList(one, null));
        argsList.add(arguments(new BatchBinding<>(bt, Vars.of("x", "y")).setRow(b, 0),
                               asList(one, null)));

        b = bt.createSingleton(2);
        b.putRow(Arrays.asList(null, two));
        argsList.add(arguments(new BatchBinding<>(bt, Vars.of("x", "y")).setRow(b, 0),
                asList(null, two)));

        b = bt.create(2, 3, 0);
        b.putRow(asList(null, null, null));
        b.putRow(Arrays.asList(one, two, three));
        argsList.add(arguments(new BatchBinding<>(bt, Vars.of("x", "y", "z")).setRow(b, 1),
                               asList(one, two, three)));
    }

    static Stream<Arguments> test() {
        List<Arguments> argsList = new ArrayList<>(List.of(
                arguments(new ArrayBinding(Vars.of("x"),
                                           new Term[]{one}),
                          List.of(one)),
                arguments(new ArrayBinding(Vars.of("x"),
                                           new Term[]{null}),
                          singletonList(null)),
                arguments(new ArrayBinding(Vars.of("x", "y"),
                                           new Term[]{one, null}),
                          asList(one, null)),
                arguments(new ArrayBinding(Vars.of("y", "x"),
                                           new Term[]{null, two}),
                          asList(null, two)),
                arguments(new ArrayBinding(Vars.of("x", "y", "z"),
                                           new Term[]{one, two, three}),
                          asList(one, two, three))
        ));
        addCases(Batch.TERM, argsList);
        addCases(Batch.COMPRESSED, argsList);
        return argsList.stream();
    }

    @ParameterizedTest @MethodSource
    void test(Binding binding, List<Term> values) {
        assertEquals(values.size(), binding.size());
        assertEquals(values.size(), binding.vars.size());
        assertEquals(values.size(), binding.vars().size());

        for (int i = 0; i < values.size(); i++) {
            Term term = values.get(i);
            assertEquals(term, binding.get(i));
            assertEquals(term, binding.get(binding.vars.get(i)));
            assertEquals(term, binding.get(Term.valueOf("?"+ binding.vars.get(i))));
        }

        assertNull(binding.get(Rope.of("notPresent")));
        assertNull(binding.get(Term.valueOf("?notPresent")));
        assertThrows(IndexOutOfBoundsException.class, () -> binding.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> binding.get(binding.size()));
        assertThrows(IndexOutOfBoundsException.class, () -> binding.get(binding.size()+1));

        var revValues = new ArrayList<>(values);
        Collections.reverse(values);
        for (int i = 0; i < binding.size(); i++)
            assertSame(binding, binding.set(i, revValues.get(i)));
        for (int i = 0; i < binding.size(); i++)
            assertEquals(revValues.get(i), binding.get(i));
    }

    @Test
    void testArrayBindingFactoryMethod() {
        var b = ArrayBinding.of("?x", "<a>", "y", "23", "$zzz", "\"bob\"@en");
        assertEquals(3, b.size());
        assertEquals(Term.iri("<a>"), b.get(Rope.of("x")));
        assertEquals(Term.iri("<a>"), b.get(Term.valueOf("?x")));
        assertEquals(Term.iri("<a>"), b.get(0));

        assertEquals(Term.typed(23, RopeDict.DT_integer), b.get(1));
        assertEquals(Term.typed(23, RopeDict.DT_integer), b.get(Rope.of("y")));
        assertEquals(Term.typed(23, RopeDict.DT_integer), b.get(Term.valueOf("?y")));

        assertEquals(Term.lang("bob", "en"), b.get(2));
        assertEquals(Term.lang("bob", "en"), b.get(Rope.of("zzz")));
        assertEquals(Term.lang("bob", "en"), b.get(Term.valueOf("?zzz")));
    }
}