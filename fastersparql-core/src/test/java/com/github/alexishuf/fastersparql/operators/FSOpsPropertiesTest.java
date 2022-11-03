package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.util.FSProperties;
import com.github.alexishuf.fastersparql.operators.reorder.AvoidCartesianJoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.NoneJoinReorderStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static com.github.alexishuf.fastersparql.operators.FSOpsProperties.bindJoinReorder;
import static com.github.alexishuf.fastersparql.operators.FSOpsProperties.hashJoinReorder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FSOpsPropertiesTest {

    @AfterEach
    void tearDown() throws IllegalAccessException {
        FSProperties.refresh();
        for (Field f : FSOpsProperties.class.getFields()) {
            if (f.getType().equals(String.class)) {
                int mods = Modifier.STATIC | Modifier.FINAL | Modifier.PUBLIC;
                if ((f.getModifiers() & mods) == mods)
                    System.clearProperty(f.get(null).toString());
            }
        }
    }

    @Test
    void testDefaultBindJoinReorder() {
        assertEquals(AvoidCartesianJoinReorderStrategy.class, bindJoinReorder().getClass());
    }

    @Test
    void testBindJoinReorderFallsBackToGeneral() {
        System.setProperty(FSOpsProperties.OP_JOIN_REORDER, "None");
        try {
            assertEquals(NoneJoinReorderStrategy.class, bindJoinReorder().getClass());
        } finally {
            System.clearProperty(FSOpsProperties.OP_JOIN_REORDER);
        }
    }

    @Test
    void testHashJoinReorderOverridable() {
        System.setProperty(FSOpsProperties.OP_JOIN_REORDER, "None");
        System.setProperty(FSOpsProperties.OP_JOIN_REORDER_HASH, "AvoidCartesian");
        try {
            assertEquals(AvoidCartesianJoinReorderStrategy.class, hashJoinReorder().getClass());
        } finally {
            System.clearProperty(FSOpsProperties.OP_JOIN_REORDER);
            System.clearProperty(FSOpsProperties.OP_JOIN_REORDER_HASH);
        }
    }

    @Test
    void testWcoJoinReorderThrowsInsteadOfFallback() {
        System.setProperty(FSOpsProperties.OP_JOIN_REORDER, "None");
        System.setProperty(FSOpsProperties.OP_JOIN_REORDER_WCO, "bullshit");
        try {
            assertThrows(IllegalArgumentException.class, FSOpsProperties::wcoJoinReorder);
        } finally {
            System.clearProperty(FSOpsProperties.OP_JOIN_REORDER);
            System.clearProperty(FSOpsProperties.OP_JOIN_REORDER_WCO);
        }
    }
}