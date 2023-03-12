package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.client.ChildJVM;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class FSPropertiesTest {
    private static final String TEST_PROP_NAME = "fastersparql.test.prop";
    private static final String TEST_ENV_NAME = "FASTERSPARQL_TEST_PROP";

    public static class PropReader {
        public static void main(String[] args) {
            Integer value = FSProperties.readProperty(TEST_PROP_NAME, 47,
                    (src, val) -> Integer.parseInt(val));
            System.out.println(value);
        }
    }

    @Test
    void testReadProp() throws Exception {
        readProp.get();
    }
    private static final CompletableFuture<?> readProp = new CompletableFuture<>();
    static {
        Thread.startVirtualThread(() -> {
            try (ChildJVM jvm = ChildJVM.builder(PropReader.class).envVar(TEST_ENV_NAME, "5")
                    .jvmArg("-D" + TEST_PROP_NAME + "=23").build()) {
                assertEquals(23, Integer.parseInt(jvm.readAllOutput().trim()));
                readProp.complete(null);
            } catch (Throwable t) {
                readProp.completeExceptionally(t);
            }
        });
    }

    @Test
    void testReadEnv() throws Exception {
        readEnv.get();
    }
    private static final CompletableFuture<?> readEnv = new CompletableFuture<>();
    static {
        Thread.startVirtualThread(() -> {
            try (ChildJVM jvm = ChildJVM.builder(PropReader.class).envVar(TEST_ENV_NAME, "5").build()) {
                assertEquals(5, Integer.parseInt(jvm.readAllOutput().trim()));
                readEnv.complete(null);
            } catch (Throwable t) {
                readEnv.completeExceptionally(t);
            }
        });
    }

    @Test
    void testReadDefault() {
        FSProperties.readProperty(TEST_PROP_NAME, 47,
                (src, val) -> Integer.parseInt(val));
    }
}