package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.client.ResultsSparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.lrb.sources.LrbSource;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PlanRegistryTest {
    private static PlanRegistry builtin;

    @BeforeAll
    static void beforeAll() {
        builtin = PlanRegistry.parseBuiltin();
        Map<String, SparqlClient> host2client = new HashMap<>();
        for (LrbSource src : LrbSource.values()) {
            String url = "http://" + src.name().toLowerCase().replace("_", "-") + "/sparql";
            host2client.put(url, new ResultsSparqlClient(false, url));
        }
        builtin.resolve(host2client);
    }

    @AfterAll
    static void afterAll() {
        builtin = null;
    }

    @ParameterizedTest @EnumSource(QueryName.class)
    void testParsePlans(QueryName qry) {
        Plan plan = builtin.createPlan(qry);
        assertNotNull(plan);
        assertEquals(qry.parsed().publicVars(), plan.publicVars());
        if (!qry.name().startsWith("B")) {
            assertEquals(new HashSet<>(qry.parsed().allVars()),
                         new HashSet<>(plan.allVars()));
        } // Some b queries received manual optmizations that drop unused internal vars
        assertEquals(plan, builtin.createPlan(qry.name()));
    }
}