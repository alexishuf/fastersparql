package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentLinkedQueue;

@SuppressWarnings("unused")
public class FS {
    private static final Logger log = LoggerFactory.getLogger(FS.class);

    /**
     * Alias for {@link FS#factory(List)} with variadic arguments
     *
     * @param preferredTagsOrClassNames list of preferred tags, fully qualified class
     *                                  names or simple class names.
     * @return the highest-scoring {@link SparqlClientFactory}. The returned factory may
     *         correspond to none of the preferred tags or class names.
     */
    public static SparqlClientFactory factory(String... preferredTagsOrClassNames) {
        return factory(Arrays.asList(preferredTagsOrClassNames));
    }

    /**
     * Get the highest-{@link SparqlClientFactory#order()} {@link SparqlClientFactory} instance
     * whose {@link SparqlClientFactory#tag()} or {@link Object#getClass()} appears
     * first in the given {@code preferredTagsOrClassNames} list.
     *
     * @param preferredTagsOrClassNames a list of {@link SparqlClientFactory#tag()}s,
     *                                  {@link Class#getName()} and {@link Class#getSimpleName()}.
     *                                  The earlier an entry appears, the higher its precedence.
     * @return a non-null {@link SparqlClientFactory}. The returned {@link SparqlClientFactory}
     *         may correspond to no tag or class name in {@code preferredTagsOrClassNames}.
     */
    public static SparqlClientFactory factory(List<String> preferredTagsOrClassNames) {
        SparqlClientFactory best = null;
        int bestIndex = Integer.MAX_VALUE, bestOrder = Integer.MAX_VALUE;
        for (SparqlClientFactory factory : ServiceLoader.load(SparqlClientFactory.class)) {
            int i = preferredTagsOrClassNames.indexOf(factory.tag());
            if (i < 0)
                i = preferredTagsOrClassNames.indexOf(factory.getClass().getName());
            if (i < 0)
                i = preferredTagsOrClassNames.indexOf(factory.getClass().getSimpleName());
            if (i < 0)
                i = Integer.MAX_VALUE-1;
            int order = factory.order();
            if (i < bestIndex) {
                bestIndex = i;
                bestOrder = order;
                best = factory;
            } else if (i == bestIndex && order < bestOrder) {
                bestOrder = order;
                best = factory;
            }
        }
        return best;
    }

    public static <R, I, F> SparqlClient<R, I, F>
    clientFor(SparqlEndpoint endpoint, RowType<R, I> rowType, FragmentParser<F> fragmentParser) {
        return factory().createFor(endpoint, rowType, fragmentParser);
    }

    public static <R, I> SparqlClient<R, I, byte[]>
    clientFor(SparqlEndpoint endpoint, RowType<R, I> rowType) {
        return factory().createFor(endpoint, rowType);
    }

    public static <F> SparqlClient<String[], String, F>
    clientFor(SparqlEndpoint endpoint, FragmentParser<F> fragmentParser) {
        return factory().createFor(endpoint, fragmentParser);
    }


    public static SparqlClient<String[], String, byte[]>
    clientFor(SparqlEndpoint endpoint) { return factory().createFor(endpoint); }

    public static <R, I, F> SparqlClient<R, I, F>
    clientFor(String augmentedUri, RowType<R, I> rowType, FragmentParser<F> fragmentParser) {
        return factory().createFor(SparqlEndpoint.parse(augmentedUri), rowType, fragmentParser);
    }

    public static <R, I> SparqlClient<R, I, byte[]>
    clientFor(String augmentedUri, RowType<R, I> rowType) {
        return factory().createFor(SparqlEndpoint.parse(augmentedUri), rowType);
    }

    public static <F> SparqlClient<String[], String, F>
    clientFor(String augmentedUri, FragmentParser<F> fragmentParser) {
        return factory().createFor(SparqlEndpoint.parse(augmentedUri), fragmentParser);
    }

    public static SparqlClient<String[], String, byte[]>
    clientFor(String augmentedUri) {
        return factory().createFor(SparqlEndpoint.parse(augmentedUri));
    }

    private static final Queue<Runnable> shutdownHooks = new ConcurrentLinkedQueue<>();

    /**
     * Register a Runnable to execute when {@link FS#shutdown()} is called.
     *
     * <p>Once {@link FS#shutdown()} is called, the hook will be de-registered after
     * execution. If resources needing a shutdown hook are re-acquired after a
     * {@link FS#shutdown()}, this method must be called again to ensure the hook
     * will execute on a second {@link FS#shutdown()}  call.</p>
     *
     * @param runnable code to execute on {@link FS#shutdown()}.
     */
    public static void addShutdownHook(Runnable runnable) { shutdownHooks.add(runnable); }

    /**
     * Releases globally held resources initialized by fastersparql components.
     *
     * <p>One example of such resources are Netty {@code EventLoopGroup} and the associated threads.
     * If this method is not called, it may take a few seconds for the {@code EventLoopGroup} to
     * be closed by its keep-alive timeout.</p>
     */
    public static void shutdown() {
        for (Runnable hook = shutdownHooks.poll(); hook != null; hook = shutdownHooks.poll()) {
            try {
                hook.run();
            } catch (Throwable t) { log.error("Failed to execute {}: ", hook, t); }
        }
    }
}
