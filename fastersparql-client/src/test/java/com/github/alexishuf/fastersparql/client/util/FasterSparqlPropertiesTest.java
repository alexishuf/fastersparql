package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.ChildJVM;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.github.alexishuf.fastersparql.client.util.async.Async.asyncThrowing;

public class FasterSparqlPropertiesTest {
    private static final String TEST_PROP_NAME = "fastersparql.test.prop";
    private static final String TEST_ENV_NAME = "FASTERSPARQL_TEST_PROP";

    public static class PropReader {
        public static void main(String[] args) {
            Integer value = FasterSparqlProperties.readProperty(TEST_PROP_NAME, 47,
                    (src, val) -> Integer.parseInt(val));
            System.out.println(value);
        }
    }

    @Test
    void testReadProp() throws ExecutionException {
        readProp.get();
    }
    private static final AsyncTask<?> readProp =
            asyncThrowing(FasterSparqlPropertiesTest::doTestReadProp);
    private static void doTestReadProp() throws IOException {
        ChildJVM jvm = ChildJVM.builder().envVar(TEST_ENV_NAME, "5")
                .jvmArg("-D" + TEST_PROP_NAME + "=23")
                .mainClass(PropReader.class).build();
        Assertions.assertEquals(23, Integer.parseInt(jvm.readAllOutput().trim()));
    }

    @Test
    void testReadEnv() throws ExecutionException {
        readEnv.get();
    }
    private static final AsyncTask<?> readEnv =
            asyncThrowing(FasterSparqlPropertiesTest::doTestReadEnv);
    private static void doTestReadEnv() throws IOException {
        ChildJVM jvm = ChildJVM.builder().envVar(TEST_ENV_NAME, "5")
                .mainClass(PropReader.class).build();
        Assertions.assertEquals(5, Integer.parseInt(jvm.readAllOutput().trim()));
    }

    @Test
    void testReadDefault() {
        FasterSparqlProperties.readProperty(TEST_PROP_NAME, 47,
                (src, val) -> Integer.parseInt(val));
    }
}