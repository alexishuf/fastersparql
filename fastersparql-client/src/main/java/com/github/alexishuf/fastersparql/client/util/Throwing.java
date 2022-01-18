package com.github.alexishuf.fastersparql.client.util;


/**
 * {@link FunctionalInterface}s that may throw {@link Exception}s in addition
 * to {@link Error}s and {@link RuntimeException}s.
 */
public class Throwing {
    /** Throwing version of {@link java.util.function.Supplier} */
    @FunctionalInterface public interface Supplier<T> {
        T get() throws Exception;
    }

    /** Throwing version of {@link java.util.function.Consumer} */
    @FunctionalInterface public interface Consumer<T> {
        void accept(T t) throws Exception;
    }

    private static final Consumer<Object> SINK = x -> {};
    @SuppressWarnings("unchecked")
    public static <T> Consumer<T> sink() { return (Consumer<T>) SINK; }

    /** Throwing version of {@link java.util.function.Function} */
    @FunctionalInterface public interface Function<T, R> {
        R apply(T t) throws Exception;
    }

    private static final Function<Object, Object> IDENTITY = x -> x;
    @SuppressWarnings("unchecked")
    public static <T> Function<T, T> identity() {return (Function<T, T>) IDENTITY;}

    /** Throwing version of {@link java.util.function.BiFunction} */
    @FunctionalInterface public interface BiFunction<T, U, R> {
        R apply(T t, U u) throws Exception;
    }

    /** Throwing version of {@link java.util.function.BiConsumer} */
    @FunctionalInterface public interface BiConsumer<T, U> {
        void accept(T t, U u);
    }

    private static final BiConsumer<Object, Object> BI_SINK = (x, y) -> {};
    @SuppressWarnings("unchecked")
    public static <T, U> BiConsumer<T, U> biSink() { return (BiConsumer<T, U>) BI_SINK; }

    /** Throwing version of {@link java.lang.Runnable} */
    @FunctionalInterface public interface Runnable {
        void run() throws Exception;
    }

    public static final Runnable NOP = () -> {};
}
