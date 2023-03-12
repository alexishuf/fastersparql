package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.fed.selectors.AskSelector;
import com.github.alexishuf.fastersparql.fed.selectors.TrivialSelector;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.util.Async;
import com.github.alexishuf.fastersparql.util.IOUtils;
import com.github.alexishuf.fastersparql.util.NamedService;
import com.github.alexishuf.fastersparql.util.NamedServiceLoader;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

import static com.github.alexishuf.fastersparql.fed.Spec.PATHS_RELATIVE_TO;

public abstract class Selector implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Selector.class);

    public static final String TYPE = "type";
    public static final String STATE = "state";
    public static final String STATE_INIT_SAVE = "init-save";
    public static final String STATE_FILE = "file";
    public static final List<String> STATE_FILE_PATH = List.of(STATE, STATE_FILE);

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
    private final Spec spec;
    private final CompletableFuture<InitOrigin> completableInit = new CompletableFuture<>();

    /* --- --- --- load --- --- ---  */
    /**
     * Creates a {@link Selector} instance from a previously {@link Selector#save(OutputStream)}d
     * state.
     *
     * @param client {@link SparqlClient} that the selector may use after the state is restored.
     * @param spec   configuration properties for the selector.
     * @param in     {@link InputStream} with the selector state serialization
     * @throws IOException               if reading from {@code in} fails.
     * @throws BadSerializationException if the serialization is invalid (empty, missing/unexpected
     *                                   selector name or invalid specifically to the {@link Selector}
     *                                   implementation serialized.
     */
    public static Selector load(SparqlClient client, Spec spec, InputStream in) throws IOException {
        ByteRope r = new ByteRope(256);
        String name = r.readLine(in) ? r.toString() : null;
        if (name == null)
            throw new BadSerializationException("Empty state file");
        String specName = spec.getOr(TYPE, AskSelector.NAME);
        if (specName != null && !name.equalsIgnoreCase(specName))
            throw new BadSerializationException.SelectorTypeMismatch(specName, name);
        return NSL.get(r.toString()).load(client, spec, in);
    }

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
        File state = spec.getFile(STATE_FILE_PATH, null);
        if (state != null && state.exists() && state.length() > 0) {
            try (var is = new FileInputStream(state)) {
                return Selector.load(client, spec, is);
            } catch (BadSerializationException.SelectorTypeMismatch e) {
                log.info("Selector state file is for a {} selector, spec requires a {} selector. Will ignore state and create a new {} selector",
                          e.expected(), e.actual(), e.expected());
            }
        } else if (state != null && !name.equalsIgnoreCase(TrivialSelector.NAME)) {
            log.info("{} not found, creating selector from scratch using {}", state, client);
        }
        return create(name, client, spec);
    }

    /**
     * Create a {@link Selector} of the given implementation without a previously save()d state.
     *
     * @param name the {@link Selector} implementation name
     * @param client {@link SparqlClient} that the implementation may use on initialization.
     * @param spec Configurations that shall apply to the selector.
     * @return a new {@link Selector} with initialization running in the background.
     * @see Selector#initialization()
     */
    public static Selector create(String name, SparqlClient client, Spec spec) {
        return NSL.get(name).create(client, spec);
    }

    private static final NamedServiceLoader<Loader, String> NSL
            = new NamedServiceLoader<>(Loader.class) {
        @Override protected Loader fallback(String name) {
            throw new BadSerializationException.UnknownSelector(name);
        }
    };

    public interface Loader extends NamedService<String> {
        /** Reads state from {@code in} and return  {@link Selector} with that state. */
        Selector load(SparqlClient client, Spec spec, InputStream in) throws IOException, BadSerializationException;
        /** Creates a Selector without previously saved state. */
        Selector create(SparqlClient client, Spec spec);
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
            completableInit.completeExceptionally(error);
        } else {
            Spec s = origin == InitOrigin.QUERY ? spec.get(STATE, Spec.class) : null;
            if (s != null && s.getBool(STATE_INIT_SAVE) && s.has(STATE_FILE)) {
                Path relTo = spec.get(PATHS_RELATIVE_TO, Path.class);
                Async.completeWhenWith(completableInit, saveIfEnabled(relTo), origin);
            } else {
                completableInit.complete(origin);
            }
        }
    }

    @Override public void close() { }

    public SparqlEndpoint endpoint() { return endpoint; }

    /**
     * A {@link Future} that will complete once the {@code this} is initialized.
     *
     * <p>If {@code this} was instantiated from saved state, it will be initialized upon
     * instantiation. However, some implementations when created via
     * {@link Loader#create(SparqlClient, Spec)} will launch background queries against the endpoint
     * and initialization will only complete after those queries.</p>
     */
    public CompletionStage<InitOrigin> initialization() { return completableInit; }

    /* --- --- --- save --- --- ---  */

    /**
     * Serialize this {@link Selector} state into {@code out} in such way that the
     * {@link Selector#load(SparqlClient, Spec, InputStream)} implementation for this {@link Selector}
     * can recreate {@link Selector} with the same state in the future.
     *
     * <p>The serialization will ways start with {@code getClass().toString()+"\n"} in
     * UTF-8. What comes next is dependent on the {@link Selector} implementation and
     * may be any type of text or binary data.</p>
     */
    public abstract void save(OutputStream out) throws IOException;

    public CompletionStage<@Nullable File> saveIfEnabled(@Nullable Path relativeTo) {
        var future = new CompletableFuture<@Nullable File>();
        File relDestination = spec.getFile(STATE_FILE_PATH, null);
        if (relDestination == null) {
            future.complete(null);
        } else {
            File destination = relDestination.isAbsolute() || relativeTo == null ? relDestination
                             : relativeTo.resolve(relDestination.toPath()).toFile();
            Thread.startVirtualThread(() -> {
                try {
                    IOUtils.writeWithTmp(destination, this::save);
                    future.complete(destination);
                } catch (Throwable t) {
                    log.error("Could not save state of {} into {}", this, destination, t);
                    future.completeExceptionally(t);
                }
            });
        }
        return future;
    }

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
