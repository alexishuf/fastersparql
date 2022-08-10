package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.util.reactive.AsyncIterable;
import com.github.alexishuf.fastersparql.client.util.reactive.EmptyPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A list of variables to bundled with a {@link Publisher} that produces solutions, each
 * represented by an instance of {@code Row}.
 *
 * <p>Any {@code Row} type should fall into one of these categories:</p>
 * <ol>
 *     <li>It is iterable (array, {@link Iterable}, {@link Stream}, etc.)</li>
 *     <li>It maps vars to bindings ({@link Map} iterable of name-value pairs, etc)</li>
 * </ol>
 *
 * <p>For {@code Row} types in the first category, iteration will yield as many values as
 * there are {@link Results#vars()} with the i-th value corresponding to
 * the i-th var. For unbound vars, {@code null} or {@code Optional.empty()} must be used.</p>
 *
 * <p>For {@code Row} types in the second category, unbound vars can be either not mapped or
 * mapped to {@code null} or {@code Optional.empty()}.</p>
 *
 * @param <Row>  the type that represents an individual solution, binding RDF terms to variables.
 */
public class Results<Row> {
    /**
     * The list of variables in the results.
     */
    public List<String> vars() { return vars; }
    private final List<String> vars;

    /**
     * The class of items produced by {@link Results#publisher()}.
     */
    public Class<? super Row> rowClass() { return rowClass; }
    private final Class<? super Row> rowClass;

    /**
     * A single-subscription {@link Publisher} that produces solutions, each providing bindings
     * to variables in {@link Results#vars()}.
     *
     * <p>If {@code Row} is iterable (e.g., array, {@link Iterable}, {@link Stream}, etc), the i-th
     * iterated value must contain a null, {@code Optional.empty()} or an RDF term corresponding
     * to the i-th variable in {@link Results#vars()}.</p>
     */
    public FSPublisher<Row> publisher() { return publisher; }
    private final FSPublisher<Row> publisher;

    public Results(List<String> vars, Class<? super Row> rowClass,
                   FSPublisher<? extends Row> publisher) {
        this.vars = vars;
        this.rowClass = rowClass;
        //noinspection unchecked
        this.publisher = (FSPublisher<Row>) publisher;
    }

    /**
     * Create a {@link Results} with zero vars and the given {@code rowClass} with an empty
     * {@link Results#publisher()} that fails with {@code cause} upon subscription.
     */
    public static <Row> Results<Row> error(Class<? super Row> rowClass, Throwable cause) {
        return error(Collections.emptyList(), rowClass, cause);
    }

    /**
     * Create a {@link Results} with given {@code vars} and {@code rowClass} with an empty
     * {@link Results#publisher()} that fails with {@code cause} upon subscription.
     */
    public static <Row> Results<Row> error(List<String> vars, Class<? super Row> rowClass,
                                           Throwable cause) {
        return new Results<>(vars, rowClass, FSPublisher.bindToAny(new EmptyPublisher<>(cause)));
    }

    /**
     * Create a {@link Results} with given {@code vars} and {@code rowClass} with an empty
     * {@link Results#publisher()}.
     */
    public static <Row> Results<Row> empty(Class<? super Row> rowClass) {
        return empty(Collections.emptyList(), rowClass);
    }

    /**
     * Create a {@link Results} with given {@code vars} and {@code rowClass} with an empty
     * {@link Results#publisher()}.
     */
    public static <Row> Results<Row> empty(List<String> vars, Class<? super Row> rowClass) {
        return new Results<>(vars, rowClass, FSPublisher.bindToAny(new EmptyPublisher<>()));
    }

    /**
     * Get a lazy {@link Iterable} wrapping the {@link Publisher}.
     *
     * <p>As {@link Results#publisher} is single-subscription, calling {@link AsyncIterable#start()}
     * will disallow subsequent calls to {@link Publisher#subscribe(Subscriber)} on
     * {@link Results#publisher()}.</p>
     *
     * @return a non-null {@link AsyncIterable} wrapping {@link Results#publisher}
     */
    public AsyncIterable<Row> iterable() {
        return new IterableAdapter<>(publisher);
    }
}
