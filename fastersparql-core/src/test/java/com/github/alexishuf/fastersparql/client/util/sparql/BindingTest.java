package com.github.alexishuf.fastersparql.client.util.sparql;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.binding.RowBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BindingTest {
    static Stream<Arguments> test() {
        Term one = Term.typed(1, RopeDict.DT_integer);
        Term two = Term.typed(2, RopeDict.DT_integer);
        Term three = Term.typed(3, RopeDict.DT_integer);
        ArrayList<Term> oneList = new ArrayList<>(List.of(one));
        ArrayList<Term> nullList = new ArrayList<>(singletonList(null));
        List<Arguments> argsList = new ArrayList<>(List.of(
                arguments(new ArrayBinding(Vars.of("x"), new Term[]{one}), List.of(one)),
                arguments(new ArrayBinding(Vars.of("x"), new Term[]{null}), singletonList(null)),
                arguments(new ArrayBinding(Vars.of("x", "y"), new Term[]{one, null}), asList(one, null)),
                arguments(new ArrayBinding(Vars.of("y", "x"), new Term[]{null, two}), asList(null, two)),
                arguments(new ArrayBinding(Vars.of("x", "y", "z"), new Term[]{one, two, three}),
                        asList(one, two, three))
        ));
        for (RowType<?> rt : List.of(RowType.ARRAY, RowType.LIST, RowType.COMPRESSED)) {
            //noinspection unchecked
            RowType<Object> rto = (RowType<Object>)rt;

            Object row = rto.builder(1).set(0, one).build();
            argsList.add(arguments(new RowBinding<>(rto, Vars.of("x")).row(row), oneList));

            row = rto.builder(1).build();
            argsList.add(arguments(new RowBinding<>(rto, Vars.of("x")).row(row),
                                   nullList));

            row = rto.builder(2).set(0, one).build();
            argsList.add(arguments(new RowBinding<>(rto, Vars.of("x", "y")).row(row),
                         asList(one, null)));

            row = rto.builder(2).set(1, two).build();
            argsList.add(arguments(new RowBinding<>(rto, Vars.of("y", "x")).row(row),
                         asList(null, two)));

            row = rto.builder(3).set(0, one).set(1, two).set(2, three).build();
            argsList.add(arguments(new RowBinding<>(rto, Vars.of("x", "y", "z")).row(row),
                                   asList(one, two, three)));
        }

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