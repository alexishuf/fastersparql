package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.fed.PatternCardinalityEstimator.DEFAULT;
import static com.github.alexishuf.fastersparql.util.Results.parseTP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PatternCardinalityEstimatorTest {
    static Stream<Arguments> test() {
        var spo = parseTP(":s :p :o");
        var sp = parseTP(":s :p ?x");
        var so = parseTP(":s ?p :o");
        var s = parseTP(":s ?p ?o");
        var po = parseTP("?s :p :o");
        var p = parseTP("?s :p ?o");
        var o = parseTP("?s ?p :o");
        var free = parseTP("?s ?p ?o");

        List<Arguments> list = new ArrayList<>();
        // free costs more than anything else
        Stream.of(spo, sp, so, s, po, p, o)
                .forEach(smaller -> list.add(arguments(free, smaller)));
        // spo is the cheapest
        Stream.of(sp, so, s, po, p, o, free)
                .forEach(bigger -> list.add(arguments(bigger, spo)));
        list.add(arguments(po, sp)); // there are more subjects than objects
        // description of a object (sp) is smaller than aoo SO pairs (p)
        list.add(arguments(p, sp));

        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void test(TriplePattern bigger, TriplePattern smaller) {
        int smallerCost = DEFAULT.estimate(smaller);
        int biggerCost = DEFAULT.estimate(bigger);
        assertTrue(smallerCost < biggerCost);
    }

    @Test void testBind() {
        Term g = Term.valueOf("<g>");
        Term s = Term.valueOf("?s"), p = Term.valueOf("?p"), o = Term.valueOf("?o");
        Vars vars = Vars.of("s", "p", "o", "x");
        record D(Binding binding, TriplePattern ref) {
            public D(Binding binding, CharSequence ref) { this(binding, parseTP(ref)); }
        }
        List<D> data = List.of(
                new D(new ArrayBinding(vars).set(s, g).set(p, g).set(o, g), "<g> <g> <g>"),
                new D(new ArrayBinding(vars).set(s, g).set(p, g), "<g> <g> ?o "),
                new D(new ArrayBinding(vars).set(s, g).set(o, g), "<g> ?p  <g>"),
                new D(new ArrayBinding(vars).set(s, g), "<g> ?p  ?o "),
                new D(new ArrayBinding(vars).set(p, g).set(o, g), "?s  <g> <g>"),
                new D(new ArrayBinding(vars).set(p, g), "?s  <g> ?o "),
                new D(new ArrayBinding(vars).set(o, g), "?s  ?p  <g>"),
                new D(new ArrayBinding(vars), "?s  ?p  ?o ")
        );
        TriplePattern free = parseTP("?s ?p ?o");
        for (int i = 0; i < data.size(); i++) {
            assertEquals(DEFAULT.estimate(data.get(i).ref),
                         DEFAULT.estimate(free, data.get(i).binding), "i="+i);
        }
    }


}