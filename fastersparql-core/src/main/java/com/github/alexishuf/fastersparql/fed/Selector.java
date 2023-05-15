package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.fed.selectors.AskSelector;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.util.NamedService;
import com.github.alexishuf.fastersparql.util.NamedServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

public abstract class Selector implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Selector.class);

    public static final String TYPE = "type";
    public static final String STATE = "state";
    public static final String STATE_INIT_SAVE = "init-save";

    public enum InitOrigin {
        /** State was loaded from a serialization */
        LOAD,
        /** State was initialized through queries to the endpoint. */
        QUERY,
        /** State was entirely defined by the {@link Spec} properties */
        SPEC,
        /** State was initialized with minimal effort. However, using the selector will
         *  change its state and improve its precision */
        LAZY
    }

    public final SparqlEndpoint endpoint;
    protected final Spec spec;
    private final CompletableFuture<InitOrigin> completableInit = new CompletableFuture<>();

    /* --- --- --- load --- --- ---  */

    /**
     * Load or create a new {@link Selector} from its {@link Spec}. If the Spec mentions a
     * {@code state.file}, will try to load state from said file if the file exists, is not empty
     * and the state corresponds to the {@link Selector} type requested in the {@link Spec}.
     *
     * @param client SparqlClient that may be used by the selector during initialization and
     *               during querying
     * @param spec set of properties of the selector. If this does not include a
     *             {@link Selector#TYPE} key, it will default to {@link AskSelector#NAME}
     * @return a {@link Selector} conforming to {@code spec}, loaded from a state file or created
     *         from scratch
     * @throws IOException if something goes wrong reading from a state file.
     * @throws BadSerializationException if
     */
    public static Selector load(SparqlClient client, Spec spec) throws IOException {
        String name = spec.getOr(TYPE, AskSelector.NAME);
        return NSL.get(name).load(client, spec);
    }

    private static final NamedServiceLoader<Loader, String> NSL
            = new NamedServiceLoader<>(Loader.class) {
        @Override protected Loader fallback(String name) {
            throw new BadSerializationException.UnknownSelector(name);
        }
    };

    public interface Loader extends NamedService<String> {
        /** Reads state from {@code in} and return  {@link Selector} with that state. */
        Selector load(SparqlClient client, Spec spec) throws IOException, BadSerializationException;
    }

    /* --- --- --- lifecycle & accessors --- --- ---  */

    public Selector(SparqlEndpoint endpoint, Spec spec) {
        this.endpoint = endpoint;
        this.spec = spec;
    }

    /** Notify this {@link Selector} state is initialized and ready to be queried with
     *  reasonable precision. This method will trigger state saving if enabled and
     *  {@code origin == QUERY}. */
    protected final void notifyInit(InitOrigin origin, Throwable error) {
        if (error != null) {
            log.error("Failed to init {} from {}", this, origin, error);
            completableInit.completeExceptionally(error);
        } else {
            Spec s = origin == InitOrigin.QUERY ? spec.get(STATE, Spec.class) : null;
            if (s != null && s.getBool(STATE_INIT_SAVE) && s.has(STATE)) {
                Thread.startVirtualThread(() -> {
                    try {
                        saveIfEnabled();
                        completableInit.complete(origin);
                    } catch (IOException e) {
                        log.error("Could not save state for {}", this, e);
                        completableInit.completeExceptionally(e);
                    }
                });
            } else {
                completableInit.complete(origin);
            }
        }
    }

    @Override public void close() { }

    public SparqlEndpoint endpoint() { return endpoint; }

    @Override public String toString() { return getClass().getSimpleName()+"("+endpoint+")"; }

    /**
     * A {@link Future} that will complete once the {@code this} is initialized.
     *
     * <p>If {@code this} was instantiated from saved state, it will be initialized upon
     * instantiation. However, some implementations when created via
     * {@link Loader#load(SparqlClient, Spec)} will launch background queries against the endpoint
     * and initialization will only complete after those queries.</p>
     */
    public CompletionStage<InitOrigin> initialization() { return completableInit; }

    /* --- --- --- save --- --- ---  */

    /**
     * Serialize this {@link Selector} state into {@code dest} in such way that the
     * {@link Selector#load(SparqlClient, Spec)} implementation for this {@link Selector}
     * can recreate {@link Selector} with the same state in the future.
     */
    public abstract void saveIfEnabled() throws IOException;

    /* --- --- --- functionality --- --- ---  */

    /**
     * Whether the endpoint MAY have answers to the given triple pattern. If this returns
     * {@code false}, then there MUST not be any answers for {@code tp} in the endpoint.
     */
    public abstract boolean has(TriplePattern tp);

    /**
     * Get a bitset with bit {@code i} is set iff {@code has(operands.get(i))}.
     *
     * <p><strong>Important:</strong> this method requires {@code operands.size() <= 64}. It will
     * silently output incorrect results otherwise</p>
     *
     * @param parent {@link Plan} of which a subset of operands is to be selected.
     * @return a bitset with the indices of triple patterns matching this selector.
     * @throws IllegalArgumentException if some operands are not {@link TriplePattern}s
     */
    public long subBitset(Plan parent) {
        long selected = 0;
        for (int i = 0, n = parent.opCount(); i < n; i++) {
            if (parent.op(i) instanceof TriplePattern tp && has(tp))
                selected |= 1L << i;
        }
        return selected;
    }

    /**
     *  Get a bitset with bit {@code i} is set iff {@code has(operands.get(i))}.
     *
     * <p><strong>Important:</strong> this method requires {@code operands.size() <= 64}. It will
     * silently output incorrect results otherwise</p>
     *
     * @param parent {@link Plan} of which a subset of operands is to be selected.
     * @return a bitset with the indices of triple patterns matching this selector.
     * @throws IllegalArgumentException if some operands are not {@link TriplePattern}s
     */
    public int subBitset(long[] dest, Plan parent) {
        int size = 0;
        for (int i = 0, n = parent.opCount(); i < n; i++) {
            if (parent.op(i) instanceof TriplePattern tp && has(tp)) {
                dest[i>>6] |= 1L << i;
                ++size;
            }
        }
        return size;
    }
}
