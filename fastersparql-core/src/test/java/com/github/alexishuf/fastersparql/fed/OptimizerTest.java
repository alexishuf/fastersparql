package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Join;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.FS.*;
import static com.github.alexishuf.fastersparql.util.Results.parseTP;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class OptimizerTest {

    private final Optimizer optimizer = new Optimizer();

    @SuppressWarnings("SuspiciousNameCombination") static Stream<Arguments> test() {
        List<Arguments> list = new ArrayList<>();
        var x_knows_bob = parseTP("?x foaf:knows :Bob");
        var x_knows_alice = parseTP("?x foaf:knows :Alice");
        var y_knows_alice = parseTP("?y foaf:knows :Alice");
        var alice_knows_x = parseTP(":Alice foaf:knows ?x");
        var alice_knows_z = parseTP(":Alice foaf:knows ?z");
        var alice_knows_bob = parseTP(":Alice foaf:knows :Bob");
        var x_knows_y = parseTP("?x foaf:knows ?y");
        var alice_age_x = parseTP(":Alice foaf:age ?x");
        var x_age_a = parseTP("?x foaf:age ?a");
        var x_age_b = parseTP("?x foaf:age ?b");
        var z_age_y = parseTP("?z foaf:age ?y");
        var x_exAge_z = parseTP("?y foaf:age ?z");
        var alice_exAge_x = parseTP(":Alice :age ?x");
        var z_exAge_y = parseTP("?z :age ?y");
        var z_knows_alice = parseTP("?z foaf:knows :Alice");
        Stream.of(
                // no filters, no joins...
                parseTP("?x a ?class"),
                project(distinct(parseTP("?x a ?class")), Vars.of("class")),
                union(x_knows_bob, x_knows_alice),
                union(x_knows_bob, y_knows_alice),

                // join already ordered
                join(alice_knows_x, x_knows_y),
                join(alice_knows_bob, alice_knows_x, x_knows_y),

                //cannot push filter
                filter(join(alice_age_x, z_age_y), "?x > ?y"),
                filter(union(join(alice_age_x, z_age_y), join(alice_exAge_x, z_exAge_y)), "?x > ?y")
        ).forEach(p -> list.add(arguments(p, p)));

        // test join reordering
        list.addAll(List.of(
        /*  9 */arguments(new Join(x_knows_y, x_knows_bob, alice_knows_bob),
                          new Join(alice_knows_bob, x_knows_bob, x_knows_y)),
        /* 10 */arguments(new Join(x_knows_y, alice_knows_bob, x_knows_bob),
                          new Join(alice_knows_bob, x_knows_bob, x_knows_y)),
        /* 11 */arguments(new Join(alice_knows_bob, x_knows_y, x_knows_bob),
                          new Join(alice_knows_bob, x_knows_bob, x_knows_y))
        ));

        /* 12 - push single filter */
        list.add(arguments(
                filter(new Join(z_age_y, z_knows_alice), "?y > 23"),
                new Join( z_knows_alice, filter(z_age_y, "?y > 23"))
        ));
        /* 13 - push single filter to two locations deep down in tree */
        list.add(arguments(
                filter(union(
                        leftJoin(new Join(z_knows_alice, alice_knows_z), z_age_y),
                        new Join(z_age_y, z_exAge_y),
                        x_knows_alice
                ), "?y > 23"),
                union(leftJoin(new Join(alice_knows_z, z_knows_alice), filter(z_age_y, "?y > 23")),
                      new Join(filter(z_age_y, "?y > 23"), filter(z_exAge_y, "?y > 23")),
                      x_knows_alice)
        ));

        /* 14 - handle tree of filters,
                keep modifier after removing all its filters
                penalize operand with zero filter vars */
        list.add(arguments(
                filter(new Join(
                         project(filter(new Join(x_knows_y, x_age_b, x_exAge_z), "?b > ?z"), Vars.of("x, a")),
                         filter(new Join(x_knows_alice, x_age_a), "?a < 23")
                ), "?a != ?b"),
                filter(new Join(
                         new Join(x_knows_alice, filter(x_age_a, "?a < 23"),
                         project(new Join(filter(new Join(x_age_b, x_exAge_z), "?b > ?z"), x_knows_y), Vars.of("x", "a")))
                ), "?a != ?b")
        ));

        return list.stream();
    }

    @ParameterizedTest @MethodSource void test(Plan in, Plan expected) {
        Plan backup = in.deepCopy();
        boolean nop = expected == in;
        Plan optimized = optimizer.optimize(in, Vars.EMPTY);
        if (nop) {
            assertEquals(backup, optimized);
            assertSame(in, optimized);
        } else {
            assertEquals(expected, optimized);
            assertNotSame(in, optimized);
        }
    }


}