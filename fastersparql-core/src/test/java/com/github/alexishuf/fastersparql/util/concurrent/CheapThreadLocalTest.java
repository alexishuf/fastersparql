package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

class CheapThreadLocalTest {
    private static final int TOUCHES = 65_536;

    static Stream<Arguments> test() {
        return Stream.of(
                arguments(1, 1),
                arguments(1, 2),
                arguments(2, 1),
                arguments(4, 4),
                arguments(1, 16),
                arguments(1, 32),
                arguments(1, 64),
                arguments(2, 16),
                arguments(2, 32),
                arguments(2, 64),
                arguments(1, 1024),
                arguments(2, 1024),
                arguments(1, 2048),
                arguments(2, 2048)
        );
    }

    private static final class Dummy {
        private final Thread owner;

        public Dummy() { this.owner = Thread.currentThread(); }

        public void touch() {
            if (Thread.currentThread() != owner)
                throw new IllegalStateException("current thread is not owner thread");
        }
    }

    @ParameterizedTest @MethodSource void test(int capacity, int threads) throws Exception {
        var tl = new CheapThreadLocal<>(Dummy::new);
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            tasks.repeat(threads, () -> {
                for (int i = 0; i < TOUCHES; i++)
                    tl.get().touch();
            });
        }
    }

}