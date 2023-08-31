package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BindingTest {
    private static final Term one = Term.typed(1, DT_integer);
    private static final Term two = Term.typed(2, DT_integer);
    private static final Term three = Term.typed(3, DT_integer);


    private static <B extends Batch<B>>
    void addCases(BatchType<B> bt, List<Arguments> argsList) {
        var b = bt.createSingleton(1);
        b.putRow(List.of(one));
        argsList.add(arguments(new BatchBinding(Vars.of("x")).attach(b, 0),
                               List.of(one)));

        b = bt.createSingleton(2);
        b.putRow(Arrays.asList(one, null));
        argsList.add(arguments(new BatchBinding(Vars.of("x", "y")).attach(b, 0),
                               asList(one, null)));

        b = bt.createSingleton(2);
        b.putRow(Arrays.asList(null, two));
        argsList.add(arguments(new BatchBinding(Vars.of("x", "y")).attach(b, 0),
                asList(null, two)));

        b = bt.create(2, 3, 0);
        b.putRow(asList(null, null, null));
        b.putRow(Arrays.asList(one, two, three));
        argsList.add(arguments(new BatchBinding(Vars.of("x", "y", "z")).attach(b, 1),
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

    @Test
    void testInvalidAttach() {
        for (var bt : List.of(Batch.TERM, Batch.COMPRESSED)) {
            var b1 = bt.createSingleton(1);
            var b2 = bt.createSingleton(2);
            var c1 = bt.createSingleton(1);
            c1.putRow(new Term[]{one});
            BatchBinding bb1 = new BatchBinding(Vars.of("x"));
            BatchBinding bb2 = new BatchBinding(Vars.of("x", "y"));
            assertThrows(IndexOutOfBoundsException.class, () -> bb1.attach(b1, 0));
            assertThrows(IndexOutOfBoundsException.class, () -> bb2.attach(b2, 0));
            assertThrows(IndexOutOfBoundsException.class, () -> bb1.attach(b1, -1));
            assertThrows(IndexOutOfBoundsException.class, () -> bb1.attach(b1, 1));
            assertThrows(IndexOutOfBoundsException.class, () -> bb1.attach(c1, -1));
            assertThrows(IndexOutOfBoundsException.class, () -> bb1.attach(c1, 1));
            assertThrows(IndexOutOfBoundsException.class, () -> bb2.attach(c1, -1));
            assertThrows(IndexOutOfBoundsException.class, () -> bb2.attach(c1, 1));
            b1.recycle();
            b2.recycle();
            c1.recycle();
        }
    }

    @ParameterizedTest @MethodSource
    void test(Binding binding, List<Term> values) {
        assertEquals(values.size(), binding.size());
        assertEquals(values.size(), binding.vars().size());
        assertEquals(values.size(), binding.vars().size());

        for (int i = 0; i < values.size(); i++) {
            Term term = values.get(i);
            assertEquals(term, binding.get(i));
            assertEquals(term, binding.get(binding.vars().get(i)));
            assertEquals(term, binding.get(Term.valueOf("?"+ binding.vars().get(i))));
        }

        assertNull(binding.get(SegmentRope.of("notPresent")));
        assertNull(binding.get(Term.valueOf("?notPresent")));
        if (!(binding instanceof BatchBinding bb) || (bb.batch != null && bb.batch.rows > 0)) {
            assertThrows(IndexOutOfBoundsException.class, () -> binding.get(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> binding.get(binding.size()));
            assertThrows(IndexOutOfBoundsException.class, () -> binding.get(binding.size() + 1));
        }

        var revValues = new ArrayList<>(values);
        Collections.reverse(values);
        if (binding instanceof ArrayBinding ab) {
            for (int i = 0; i < binding.size(); i++)
                assertSame(binding, ab.set(i, revValues.get(i)));
        }
        for (int i = 0; i < binding.size(); i++)
            assertEquals(revValues.get(i), binding.get(i));
    }

    @Test
    void testArrayBindingFactoryMethod() {
        var b = ArrayBinding.of("?x", "<a>", "y", "23", "$zzz", "\"bob\"@en");
        assertEquals(3, b.size());
        assertEquals(Term.iri("<a>"), b.get(SegmentRope.of("x")));
        assertEquals(Term.iri("<a>"), b.get(Term.valueOf("?x")));
        assertEquals(Term.iri("<a>"), b.get(0));

        assertEquals(Term.typed(23, DT_integer), b.get(1));
        assertEquals(Term.typed(23, DT_integer), b.get(SegmentRope.of("y")));
        assertEquals(Term.typed(23, DT_integer), b.get(Term.valueOf("?y")));

        assertEquals(Term.lang("bob", "en"), b.get(2));
        assertEquals(Term.lang("bob", "en"), b.get(SegmentRope.of("zzz")));
        assertEquals(Term.lang("bob", "en"), b.get(Term.valueOf("?zzz")));
    }
}