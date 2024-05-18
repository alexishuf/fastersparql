package com.github.alexishuf.fastersparql.util;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadFailedException;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.client.*;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Emitters;
import com.github.alexishuf.fastersparql.emit.ReceiverErrorFuture;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeFactory;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.Query;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;
import com.github.alexishuf.fastersparql.sparql.parser.PrefixMap;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.concurrent.Watchdog;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Guard.BatchGuard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.SpecialOwner;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.*;

import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static com.github.alexishuf.fastersparql.sparql.parser.SparqlParser.parse;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.IntStream.range;

@SuppressWarnings("unused")
public final class Results {
    private static final Logger log = LoggerFactory.getLogger(Results.class);
    private static boolean primed = false;
    public static final PrefixMap PREFIX_MAP;
    static {
        PrefixMap pm = PrefixMap.create().takeOwnership(SpecialOwner.CONSTANT).resetToBuiltin();
        pm.add(FinalSegmentRope.asFinal(""),     Term.valueOf("<http://example.org/>"));
        pm.add(FinalSegmentRope.asFinal("ex"),   Term.valueOf("<http://example.org/>"));
        pm.add(FinalSegmentRope.asFinal("exns"), Term.valueOf("<http://www.example.org/ns#>"));
        pm.add(FinalSegmentRope.asFinal("rdfs"), Term.valueOf("<http://www.w3.org/2000/01/rdf-schema#>"));
        pm.add(FinalSegmentRope.asFinal("owl"),  Term.valueOf("<http://www.w3.org/2002/07/owl#>"));
        pm.add(FinalSegmentRope.asFinal("foaf"), Term.valueOf("<http://xmlns.com/foaf/0.1/>"));
        PREFIX_MAP = pm;
    }

    private final @Nullable Vars vars;
    private final int columns;
    private final List<List<Term>> expected;
    private final boolean ordered;
    private final DuplicatesPolicy duplicatesPolicy;
    private final @Nullable Class<? extends Throwable> expectedError;
    private final @Nullable SparqlQuery query;
    private final @Nullable List<List<Term>> bindingsList;
    private final Vars bindingsVars;
    private final BindType bindType;
    private final String context;

    public enum DuplicatesPolicy {
        /** There must be no duplicate rows in the result */
        REQUIRE_DEDUP,
        /** The result may miss some or all duplicate instances, but must not contain
         *  more duplicates instances than expected for any given row */
        ALLOW_DEDUP,
        /** The result may contain more duplicates than expected for any given row, but it cannot
         * contain less duplicate instances than expected. */
        ALLOW_MORE_DUPLICATES,
        /** For any given row, results must contain the exact number of expected duplicates. */
        EXACT
    }

    private Results(@Nullable Vars vars, Collection<?> expected, boolean ordered,
                    DuplicatesPolicy duplicatesPolicy,
                    @Nullable Class<? extends Throwable> expectedError,
                    @Nullable SparqlQuery query,
                    @Nullable Vars bindingsVars,
                    @Nullable List<List<Term>> bindingsList,
                    @Nullable BindType bindType,
                    @Nullable String context) {
        if (!primed) {
            primed = true;
            Primer.INSTANCE.sync();
        }
        if (expected.stream().anyMatch(Objects::isNull))
            throw new IllegalArgumentException("null rows in expected");
        this.expected = expected.stream().map(Results::normalizeRow).toList();
        List<Integer> widths = this.expected.stream().map(Collection::size).distinct().toList();
        if (widths.size() > 1)
            throw new IllegalArgumentException("Non-uniform width of rows in expected");
        if (vars != null && !widths.isEmpty() && vars.size() != widths.getFirst())
            throw new IllegalArgumentException("Expecting "+vars.size()+" vars, but rows have "+widths.getFirst()+" columns");
        this.vars = vars;
        this.columns = vars != null ? vars.size() : (widths.isEmpty() ? -1 : widths.getFirst());
        this.ordered = ordered;
        this.duplicatesPolicy = duplicatesPolicy;
        this.expectedError = expectedError;
        this.query = query;
        if (bindingsVars == null)
            bindingsVars = Vars.EMPTY;
        if (bindingsList == null && !bindingsVars.isEmpty())
            throw new IllegalArgumentException("null bindingsList with non-empty bindingsVars");
        if (bindingsList != null) {
            List<Integer> bWidths = bindingsList.stream().map(List::size).distinct().toList();
            if (bWidths.size() > 1)
                throw new IllegalArgumentException("Non-uniform width for bindings");
            if (!bWidths.isEmpty() && (bindingsVars.size() != bWidths.getFirst()))
                throw new IllegalArgumentException("bindingsVars do not match bindingsList width");
        }
        this.bindingsVars = bindingsVars;
        this.bindingsList = bindingsList;
        this.bindType = bindType == null ? BindType.JOIN : bindType;
        this.context = context == null ? "" : context;
    }

    /* --- --- --- static constructors --- --- --- */

    /**
     * Builds a {@link Vars} set from the leading elements of {@code varsAndTerms} that are
     * {@link CharSequence}s starting with '?' or that are themselves {@link Vars} instances and
     * use the remainder of {@code varsAndTerms} as a row-major enumeration of all terms in
     * the results. If there are {@code n} vars, every sequence of {@code n} objects will be
     * parsed by {@link TermParser} if non-null and used to build a row of expected results.
     *
     * <p>Examples:</p>
     *
     * <pre>
     *     results(Vars.of("x"), 1, 2) // two rows: [["1"^^xsd:integer], ["2"^^xsd:integer]]
     *     results("?x, 1, 2),
     *     results("?x", "1", "2"),
     *     results("?x", Term.typed(1, RopeDict.DT_integer), Term.typed(1, RopeDict.DT_integer)),
     *     results(Vars.of("x"), List.of(List.of(1), List.of(2)))
     *     results(Vars.of("x"), List.of(List.of(Term.typed(1, RopDict.DT_integer)), List.of(2)))
     *     results("?x", List.of(List.of(1), List.of(2)))
     *
     *     results("?x ?y", ":Alice", null) // single row: [[ex:Alice, null]]
     *     results("?x", "?y", ":Alice", null) // single row: [[ex:Alice, null]]
     *     results(Vars.of("x"), "?y", ":Alice", null) // single row: [[ex:Alice, null]]
     * </pre>
     */
    public static Results results(Object... varsAndTerms) {
        Vars.Mutable vars = new Vars.Mutable(10);
        int start = 0;
        while (start < varsAndTerms.length) {
            var maybeVar = varsAndTerms[start++];
            if (maybeVar instanceof Vars set) {
                vars.addAll(set);
            } else if (maybeVar instanceof CharSequence cs && !cs.isEmpty() && cs.charAt(0) == '?') {
                String[] components = cs.toString().split("[ ,]+");
                if (!Arrays.stream(components).allMatch(c -> c.startsWith("?")))
                    throw new IllegalArgumentException("Var name with , or spaces");
                for (String component : components)
                    vars.add(Term.valueOf(component));
            } else {
                --start;
                break;
            }
        }
        int columns = vars.size();
        List<List<Term>> expected;
        var terms = Arrays.copyOfRange(varsAndTerms, start, varsAndTerms.length);
        if (terms.length == 1 && terms[0] instanceof Collection<?> coll) {
            if (!coll.isEmpty() && !(coll.iterator().next() instanceof Collection))
                throw new IllegalArgumentException("If a single collection is given, it must be a Collection of Collection (rows).");
            expected = normalizeExpected(coll);
        } else {
            boolean listOfRows = Arrays.stream(terms).allMatch(o -> o instanceof Collection<?>
                    || o instanceof Object[] || o instanceof byte[]);
            if (listOfRows)
                expected = normalizeExpected(asList(terms));
            else
                expected = groupRows(columns, terms);
        }
        return new Results(vars, expected, false, DuplicatesPolicy.EXACT, null, null, null, null, BindType.JOIN, null);
    }

    public static Results results(Vars vars, Object... terms) {
        Object[] a = new Object[1 + terms.length];
        a[0] = vars;
        System.arraycopy(terms, 0, a, 1, terms.length);
        return results(a);
    }

    private static List<List<Term>> normalizeExpected(Collection<?> collection) {
        if (collection.isEmpty()) return List.of();
        List<List<Term>> list = new ArrayList<>();
        for (Object row : collection)
            list.add(normalizeRow(row));
        return list;
    }

    private static List<List<Term>> groupRows(int columns, Object[] terms) {
        List<List<Term>> rows = new ArrayList<>();
        try (var termParserGuard = new Guard<TermParser>(GROUP_ROWS)) {
            var termParser = termParserGuard.set(TermParser.create()).eager();
            termParser.prefixMap().resetToCopy(PREFIX_MAP);
            List<Term> row = new ArrayList<>();
            for (Object term : terms) {
                row.add(term == null ? null : termParser.parseTerm(FinalSegmentRope.asFinal(term)));
                if (row.size() == columns) {
                    rows.add(row);
                    row = new ArrayList<>();
                }
            }
            if (!row.isEmpty())
                throw new IllegalArgumentException("Expected "+columns+" columns, but last row has only "+row.size()+" terms");
        }
        return rows;
    }
    private static final StaticMethodOwner GROUP_ROWS = new StaticMethodOwner("Results.groupRows");

    /** Create an {@link Results} for a {@code false} ASK query result */
    public static Results negativeResult() { return results(List.of()); }

    /** Create an {@link Results} for a {@code true} ASK query result */
    public static Results positiveResult() { return results(List.of(List.of())); }

    public static TriplePattern parseTP(CharSequence cs) {
        try (var parserGuard = new Guard<TermParser>(PARSE_TP)) {
            TermParser parser = parserGuard.set(TermParser.create()).eager();
            parser.prefixMap().resetToCopy(PREFIX_MAP);
            var r = FinalSegmentRope.asFinal(cs);
            int len = r.len();
            return new TriplePattern(parser.parseTerm(r, 0, len),
                    parser.parseTerm(r, r.skipWS(parser.termEnd(), len), len),
                    parser.parseTerm(r, r.skipWS(parser.termEnd(), len), len)
            );
        }
    }
    private static final StaticMethodOwner PARSE_TP = new StaticMethodOwner("Results.parseTP");

    /* --- --- --- variant constructors --- --- --- */

    /** Create a copy of this where the {@link BIt} rows will be expected in the same order as
     * specified when this {@link Results} was constructed. */
    public Results ordered() {
        return new Results(vars, expected, true, duplicatesPolicy, expectedError, query, bindingsVars, bindingsList, bindType, context);
    }
    /** Create a copy of this where the {@link BIt} rows will be expected in the no particular order. */
    public Results unordered() {
        return new Results(vars, expected, false, duplicatesPolicy, expectedError, query, bindingsVars, bindingsList, bindType, context);
    }
    /** Create a copy of {@code this} that will expect the given {@link BIt#vars()} */
    public Results vars(Vars vars) {
        return new Results(vars, expected, ordered, duplicatesPolicy, expectedError, query, bindingsVars, bindingsList, bindType, context);
    }
    /** Create a copy of {@code this} that will check for duplicates under the given policy */
    public Results duplicates(DuplicatesPolicy duplicatesPolicy) {
        return new Results(vars, expected, ordered, duplicatesPolicy, expectedError, query, bindingsVars, bindingsList, bindType, context);
    }
    /** Create a copy of {@code this} that will expect the given error to be thrown by the {@link BIt} */
    public Results error(@Nullable Class<? extends Throwable> error) {
        return new Results(vars, expected, ordered, duplicatesPolicy, error, query, bindingsVars, bindingsList, bindType, context);
    }
    /** Create a copy of {@code this} that will display the given context string on failure messages. */
    public Results context(String context) {
        return new Results(vars, expected, ordered, duplicatesPolicy, expectedError, query, bindingsVars, bindingsList, bindType, context);
    }

    /** Equivalent to {@link Results#contextualize(List)} on {@code Arrays.asList(results)}. */
    public static List<Results> resultsList(Results... results) {
        return contextualize(asList(results));
    }

    /** Set {@link Results#context(String)} for every {@link Results} object in {@code list}. */
    public static List<Results> contextualize(List<Results> list) {
        for (int i = 0; i < list.size(); i++)
            list.set(i, list.get(i).context("data["+i+"]"));
        return list;
    }

    /** Create a copy of {@code this} that will send the given {@link SparqlQuery}
     *  (or {@link Rope}/{@link CharSequence} to be parsed as one) to
     *  {@link SparqlClient#query(BatchType, SparqlQuery)} or {@link SparqlClient#query(ItBindQuery)}
     *  on {@link Results#check(SparqlClient)}. */
    public <R> Results query(Object sparql) {
        SparqlQuery query;
        if (sparql instanceof SparqlQuery q) {
            query = q;
        } else {
            String prologue = """
                    PREFIX     : <http://example.org/>
                    PREFIX exns: <http://www.example.org/ns#>
                    PREFIX  xsd: <http://www.w3.org/2001/XMLSchema#>
                    PREFIX  rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX  owl: <http://www.w3.org/2002/07/owl#>
                    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                    """;
            var cs = sparql instanceof CharSequence s ? s : sparql.toString();
            query = parse(RopeFactory.make(prologue.length() + RopeFactory.requiredBytes(cs))
                                     .add(prologue).add(cs).take());
        }
        return new Results(vars, expected, ordered, duplicatesPolicy, expectedError, query, bindingsVars, bindingsList, bindType, context);
    }

    /**
     * Create a copy of {@code this} that will {@code results(varsAndTerms)} as left-side bindings
     * to the {@link Results#query()} if {@link Results#check(SparqlClient)} is invoked.
     *
     * @param varsAndTerms A list of objects where any non-null object can have its
     *                     {@link Object#toString()} representation parsed by {@link TermParser}
     *                     as valid non-var {@link Term}. Such list may be prefixed by {@link Vars}
     *                     instances and space-delimited strings of '?'-vars, which will be
     *                     concatenated (removing duplicates) to build the set of vars of
     *                     the bindings. See {@link Results#results(Object...)} fore details and
     *                     examples on how this will be parsed.
     */
    public Results bindings(Object... varsAndTerms) {
        Results r = results(varsAndTerms);
        return new Results(vars, expected, ordered, duplicatesPolicy, expectedError, query,
                           r.vars(), r.expected(), bindType, context);
    }

    /** Create a copy of {@code this} with {@link #hasBindings()} returning false. */
    public Results noBindings() {
        return new Results(vars, expected, ordered, duplicatesPolicy, expectedError, query,
                null, null, bindType, context);
    }

    /** Create a copy of {@code this} that will send {@code bindType} to
     *  {@link SparqlClient#query(ItBindQuery)} on {@link Results#check(SparqlClient)}. */
    public Results bindType(BindType bindType) {
        return new Results(vars, expected, ordered, duplicatesPolicy, expectedError, query, bindingsVars, bindingsList, bindType, context);
    }

    /**
     * Create a copy of {@code this} including only rows {@code from} (inclusive) to {@code to}
     * (not inclusive) but retaining all other properties.
     */
    public Results sub(int from, int to) {
        if (from < 0 || to > expected.size())
            throw new IndexOutOfBoundsException(from < 0 ? from : to);
        return new Results(vars, expected.subList(from, to), ordered, duplicatesPolicy,
                                   expectedError, query, bindingsVars, bindingsList, bindType, context);
    }

    /** Create a copy of {@code this} only with the given columns (zero based indices). */
    public Results projection(int... columns) {
        List<List<Term>> projected = new ArrayList<>();
        for (List<Term> in : expected) {
            ArrayList<Term> out = new ArrayList<>();
            for (int col : columns)
                out.add(in.get(col));
            projected.add(out);
        }
        var projectedVars = new Vars.Mutable(columns.length);
        var vars = vars();
        for (int col : columns)
            projectedVars.add(vars.get(col));
        return new Results(projectedVars, projected, ordered, duplicatesPolicy, expectedError,
                           query, bindingsVars, bindingsList, bindType, context);
    }


    /* --- --- --- accessors & converters --- --- --- */

    public boolean            isEmpty()      { return expected.isEmpty(); }
    public boolean            isAsk()        { return vars().isEmpty() && size() <= 1; }
    public int                size()         { return expected.size(); }
    public int                columns()      { return columns; }
    public SparqlQuery        query()        { return query; }
    public List<List<Term>>   expected()     { return expected; }

    public BIt<TermBatch>     asBIt()        { return asPlan().execute(TERM); }
    public BindType           bindType()     { return bindType; }
    public boolean            hasBindings()  { return bindingsList != null; }
    public Vars               bindingsVars() { return bindingsVars; }

    public Orphan<? extends Emitter<TermBatch, ?>> asEmitter() {
        return asPlan().emit(TERM, Vars.EMPTY);
    }

    public Plan asPlan() {
        if (expectedError != null)
            throw new IllegalStateException("asPlan() does not support expectedError");
        return FS.values(vars(), expected);
    }

    public Vars vars() {
        return vars != null ? vars
                            : Vars.from(range(0, max(0, columns)).mapToObj(i->"x"+i).toList());
    }

    public List<List<Term>> bindingsList() {
        if (!hasBindings()) throw new UnsupportedOperationException("No bindings set");
        return bindingsList;
    }
    public BIt<TermBatch>  bindingsBIt()  {
        if (bindingsList == null)
            return new EmptyBIt<>(TERM, bindingsVars);
        return new IteratorBIt<>(bindingsList, TERM, bindingsVars);
    }
    public Orphan<? extends Emitter<TermBatch, ?>> bindingsEmitter() {
        if (bindingsList == null)
            return Emitters.empty(TERM, bindingsVars);
        var b = TERM.create(bindingsVars.size()).takeOwnership(this);
        for (List<Term> row : bindingsList)
            b.putRow(row);
        return Emitters.ofBatch(bindingsVars, b.releaseOwnership(this));
    }
    public ItBindQuery<TermBatch> asBindQuery() {
        if (bindingsList == null)
            throw new UnsupportedOperationException("No bindings set");
        if (query == null)
            throw new UnsupportedOperationException("No query set");
        return new ItBindQuery<>(query, bindingsBIt(), bindType);
    }

    public <B extends Batch<B>> ItBindQuery<B> asBindQuery(BatchType<B> batchType) {
        if (bindingsList == null)
            throw new UnsupportedOperationException("No bindings set");
        if (query == null)
            throw new UnsupportedOperationException("No query set");
        return new ItBindQuery<>(query, batchType.convert(bindingsBIt()), bindType);
    }

    public Results bindingsAsResults() {
        if (bindingsList == null || bindingsVars == null)
            throw new IllegalStateException("No bindings!");
        return results(bindingsVars, bindingsList);
    }

    /* --- --- --- check() methods --- --- --- */

    /** Equivalent to {@link Results#check(SparqlClient, BatchType)} with {@link TermBatchType#TERM}. */
    public void check(SparqlClient client) throws AssertionError {
        check(client, TERM);
    }

    /**
     * {@link Results#check(BIt)} on the result of querying {@link Results#query()}
     * (with bindings, if {@link Results#hasBindings()})  against client and receiving rows
     * using the given {@code rowType}.
     */
    public <B extends Batch<B>> void check(SparqlClient client,
                                           BatchType<B> batchType) throws AssertionError {
        if (query == null)
            throw new IllegalStateException("No query defined, cannot check(SparqlClient)");
        SparqlQuery query = this.query;
        if (query instanceof Plan plan)
            query = plan.transform(unboundTransformer, client);
        if (bindingsList != null) {
            bindAndCheck(client, batchType);
        } else {
            Plan oldParsed = parse(query);
            int oldHash = query.hashCode();
            String oldString = query.toString();
            check(client.query(batchType, query));
            if (!oldString.equals(query.toString()) || oldHash != query.hashCode()
                    || !oldParsed.equals(parse(query))) {
                throw new AssertionError("Query changed by "+client);
            }
            var emitter = client.emit(batchType, query, Vars.EMPTY);
            check(emitter);
            if (!oldString.equals(query.toString()) || oldHash != query.hashCode()
                    || !oldParsed.equals(parse(query))) {
                throw new AssertionError("Query changed by "+client);
            }
            if (client instanceof ResultsSparqlClient rsc)
                rsc.assertNoErrors();
        }
    }

    private <B extends Batch<B>> void bindAndCheck(SparqlClient client, BatchConverter<B> bindingsConverter) {
        bindAndCheck(client, bindingsConverter, requireNonNull(this.query), this.bindType);
    }

    private <B extends Batch<B>> void bindAndCheck(SparqlClient client,
                                                   BatchConverter<B> bindingsConverter,
                                                   SparqlQuery query, BindType bindType) {
        assert bindingsList != null;
        var observedSeq = new BitSet();
        var errors = new StringBuilder();
        String oldString = query.toString();
        int oldHash = query.hashCode();
        Plan oldParsed = parse(query);
        BIt<B> bindingsBIt = bindingsConverter.convert(bindingsBIt());
        check(client.query(new ItBindQuery<B>(query, bindingsBIt, bindType) {
            public void binding(long seq) {
                if (seq >= bindingsList.size() || seq < 0) {
                    errors.append("Invalid seq number: ").append(seq).append('\n');
                } else if (observedSeq.get((int)seq)) {
                    errors.append("Duplicate seq: ").append(seq).append('\n');
                } else {
                    observedSeq.set((int)seq);
                }
            }
            @Override public void    emptyBinding(long sequence) {binding(sequence);}
            @Override public void nonEmptyBinding(long sequence) {binding(sequence);}
        }));
        check(query, observedSeq, errors, oldHash, oldString, oldParsed);

        observedSeq.clear();
        errors.setLength(0);
        var bindingsEmitter = bindingsConverter.convert(bindingsEmitter());
        var emitter = client.emit(new EmitBindQuery<B>(query, bindingsEmitter, bindType) {
            public void binding(long seq) {
                if (seq >= bindingsList.size() || seq < 0)
                    errors.append("Invalid seq number: ").append(seq).append('\n');
                else if (observedSeq.get((int) seq))
                    errors.append("Duplicate seq: ").append(seq).append('\n');
                else
                    observedSeq.set((int) seq);
            }

            @Override public void emptyBinding(long sequence) {
                binding(sequence);
            }

            @Override public void nonEmptyBinding(long sequence) {
                binding(sequence);
            }
        }, Vars.EMPTY);
        check(emitter);
        check(query, observedSeq, errors, oldHash, oldString, oldParsed);

        if (client instanceof ResultsSparqlClient rsc)
            rsc.assertNoErrors();
    }

    private void check(SparqlQuery query, BitSet observedSeq, StringBuilder errors,
                       int oldHash, String oldString, Plan oldParsed) {
        assert bindingsList != null;
        for (int i = 0; i < bindingsList.size(); i++) {
            if (!observedSeq.get(i))
                errors.append("No *Binding(").append(i).append(") call\n");
        }
        if (oldHash != query.hashCode() || !oldString.equals(query.toString())
                || !oldParsed.equals(parse(query))) {
            errors.append("Query mutated by client.\nBefore:\n")
                    .append(oldString.replace("\n", "\n  "))
                    .append("\nAfter:\n")
                    .append(query.toString().replace("\n", "\n  "));
        }
        if (!errors.isEmpty()) {
            if (context != null && !context.isEmpty())
                throw new AssertionError("Context: "+context+"\n"+errors);
            else
                throw new AssertionError(errors.toString());
        }
    }


    /**
     * {@link Results#check(BIt)} on the result of querying {@link Results#query()}
     * (with bindings, if {@link Results#hasBindings()})  against client and receiving rows
     * using the given {@code rowType}.
     */
    public <B extends Batch<B>> void check(SparqlClient client, BatchType<B> batchType,
                                           BatchConverter<B> bindingsConverter) {
        if (query == null)
            throw new IllegalStateException("No query defined, cannot check(SparqlClient)");
        SparqlQuery query = this.query;
        if (query instanceof Plan plan)
            query = plan.transform(unboundTransformer, client);
        if (bindingsList != null) {
            bindAndCheck(client, bindingsConverter);
        } else {
            check(client.query(batchType, query));
            var emitter = client.emit(batchType, query, Vars.EMPTY);
            check(emitter);
        }
    }

    private static final Plan.Transformer<SparqlClient> unboundTransformer = new Plan.Transformer<>() {
        @Override public Plan before(Plan plan, SparqlClient client) {
            if (plan instanceof Query q && q.client instanceof UnboundSparqlClient)
                return new Query(q.sparql, client);
            return plan; // copy and transform operands
        }
    };

    /** Equivalent to {@link Results#check(BIt)} on {@code client.query(q)} */
    public void check(SparqlClient client, SparqlQuery q) throws AssertionError {
        check(client.query(TERM, q));
        var emitter = client.emit(TERM, q, Vars.EMPTY);
        check(emitter);
    }


    /** Equivalent to {@code check(((Plan)query()).execute())}. */
    public void check() {
        if (query == null)
            throw new IllegalArgumentException("no query() set for "+this);
        BIt<TermBatch> it;
        try {
            it = ((Plan) query).execute(TERM);
        } catch (Throwable t) {
            var msg = "Cannot ((Plan)query).execute() for "+this+": "
                    + t.getClass().getSimpleName()
                    + (t.getMessage() == null ? "" : ": " + t.getMessage());
            throw new IllegalArgumentException(msg, t);
        }
        check(it);
    }

    /** Consume {@code it} and check the results (and any Throwable) against this {@link Results} spec */
    public <B extends Batch<B>> void check(BIt<B> it) throws AssertionError {
        List<List<Term>> acList = new ArrayList<>();
        Throwable thrown = null;
        try (var w = Watchdog.spec("check.10s").threadStdErr(100).streamNode(it).startSecs(10)) {
            try (BatchGuard<B> guard = new BatchGuard<>(this)) {
                for (B b; (b = guard.nextBatch(it)) != null; ) {
                    for (var n = b; n != null; n = n.next) {
                        for (int i = 0; i < n.rows; i++)
                            acList.add(normalizeRow(n, i));
                    }
                }
            } catch (Throwable t) {
                w.stopAndTrigger();
                thrown = t;
            }
        }
        check(acList, thrown, it.vars());
    }

    public final class ResultsChecker<B extends Batch<B>>
            extends ReceiverErrorFuture<B, ResultsChecker<B>>
            implements Orphan<ResultsChecker<B>> {
        private final List<List<Term>> acList = new ArrayList<>();

        @Override public ResultsChecker<B> takeOwnership(Object o) {
            return sidecar.takeOwnership(o);
        }

        public Emitter<B, ?> upstream() { return upstream; }

        public void assertNoError() {
            try (var w = Watchdog.spec("check.10s").threadStdOut(100).streamNode(this).startSecs(10)) {
                Throwable error = getSimple();
                if (error != null) {
                    w.stopAndTrigger();
                    throw new AssertionError(error);
                }
            }
        }

        @Override public void onBatch(Orphan<B> orphan) {
            var b = orphan.takeOwnership(this);
            onBatchByCopy(b);
            b.recycle(this);
        }

        @Override public void onBatchByCopy(B b) {
            for (var n = b; n != null; n = n.next) {
                for (int r = 0; r < n.rows; r++)
                    acList.add(normalizeRow(n, r));
            }
        }

        @Override public boolean complete(Throwable error) {
            try {
                Results.this.check(acList, error, requireNonNull(upstream).vars());
                return super.complete(null);
            } catch (Throwable t) {
                return super.complete(t);
            }
        }
    }

    public <B extends Batch<B>> Orphan<ResultsChecker<B>>
    checker(Orphan<? extends Emitter<B, ?>> emitter) {
        ResultsChecker<B> checker = new ResultsChecker<>();
        checker.subscribeTo(emitter);
        return checker;
    }

    public <B extends Batch<B>> void check(Orphan<? extends Emitter<B, ?>> emitter) {
        ResultsChecker<B> c;
        try  (var g = new Guard<ResultsChecker<B>>(this)){
            c = g.set(checker(emitter));
            c.assertNoError();
        }
        assert !c.isAliveAndMarking();
    }

    private <B extends Batch<B>> void check(List<List<Term>> acList,
                                            Throwable thrown, Vars vars) {
        LinkedHashMap<List<Term>, Integer> ac = new LinkedHashMap<>(), ex = new LinkedHashMap<>();
        count(ex, expected);
        count(ac, acList);

        var sb = new StringBuilder(0);
        boolean ok = checkMissing(ac, ex, sb) & checkUnexpected(ac, ex, sb);
        if (!ok) {
            sb.append(format("Expected %d rows (%d unique) got %d rows (%d unique)\n",
                    expected.size(), ex.keySet().size(),
                    acList.size(), ac.keySet().size()));
        }
        if (ordered && ok && !new ArrayList<>(ac.keySet()).equals(new ArrayList<>(ex.keySet()))) {
            ok = false;
            sb.append("Mismatched order (ignoring duplicates)\n  Expected:\n");
            for (List<Term> row : ex.keySet())
                sb.append("    ").append(row).append('\n');
            sb.append("  Actual:\n");
            for (List<Term> row : ac.keySet())
                sb.append("    ").append(row).append('\n');
        }
        ok = ok & checkDuplicates(ac, ex, sb);
        ok = ok & checkException(thrown, sb);
        ok = ok & checkVars(vars, sb);
        if (!ok) {
            if (!context.isEmpty())
                sb.append("Context: ").append(context).append(context.length() > 40 ? "\n" : ". ");
            throw new AssertionError(sb.toString());
        }
    }

    @Override public String toString() {
        var sb = new StringBuilder("Results").append(vars())
                .append('{').append(expected.size()).append(" rows")
                .append(ordered ? ", ordered" : "");
        if (!context.isEmpty())
            sb.append(", ctx=").append(context);
        if (expectedError != null)
            sb.append(", expectedError=").append(expectedError.getSimpleName());
        if (bindType != BindType.JOIN)
            sb.append(", bindType=").append(bindType);
        if (bindingsVars != null && !bindingsVars.isEmpty())
            sb.append(", bindingsVars=").append(bindingsVars);
        if (bindingsList != null)
            sb.append(", #bindings=").append(bindingsList.size());
        return sb.append('}').toString();
    }

    public static List<Term> normalizeRow(Batch<?> batch, int row) {
        ArrayList<Term> list = new ArrayList<>(batch.cols);
        for (int c = 0; c < batch.cols; c++)
            list.add(batch.get(row, c));
        return list;
    }

    public static List<Term> normalizeRow(Object row) {
        try (var pGuard = new Guard<TermParser>(NORMALIZE_ROW)) {
            var p = pGuard.set(TermParser.create()).eager();
            p.prefixMap().resetToCopy(PREFIX_MAP);
            return switch (row) {
                case Collection<?> l -> {
                    if (l instanceof List<?> && l.stream().allMatch(o -> o == null || o instanceof Term)) //noinspection unchecked
                        yield (List<Term>) l;
                    var terms = new ArrayList<Term>();
                    for (Object o : l) {
                        terms.add(o == null || o instanceof Term t ? (Term) o : p.parseTerm(FinalSegmentRope.asFinal(o)));
                    }
                    yield terms;
                }
                case Term[] a -> asList(a);
                case Batch<?> b -> {
                    if (b.rows != 1 || b.next != null)
                        throw new IllegalArgumentException("Cannot normalize non-singleton batch as row");
                    var list = new ArrayList<Term>(b.cols);
                    for (int c = 0; c < b.cols; c++)
                        list.add(b.get(0, c));
                    yield list;
                }
                case int[] a -> {
                    var terms = new ArrayList<Term>();
                    try (PooledMutableRope tmp = PooledMutableRope.get()) {
                        for (int i : a) {
                            tmp.clear().append('"').append(i).append(DT_integer);
                            terms.add(Term.valueOf(tmp));
                        }
                    }
                    yield terms;
                }
                case Object[] a -> {
                    var terms = new ArrayList<Term>();
                    for (Object o : a) {
                        terms.add(switch (o) {
                            case null   -> null;
                            case Term t -> t;
                            default     -> p.parseTerm(FinalSegmentRope.asFinal(o));
                        });
                    }
                    yield terms;
                }
                case null -> throw new AssertionError("null is not a valid row object");
                default -> throw new AssertionError("Unexpected row object of type" + row.getClass().getSimpleName() + ": " + row);
            };
        }
    }
    private static final StaticMethodOwner NORMALIZE_ROW = new StaticMethodOwner("Results.normalizeRow");

    private static String toString(List<Term> row) {
        try (var sb = PooledMutableRope.get()) {
            sb.append('[');
            for (Term t : row) {
                if (t == null)
                    sb.append("null");
                else
                    t.toSparql(sb, PrefixAssigner.CANON);
                sb.append(", ");
            }
            if (!row.isEmpty())
                sb.unAppend(2);
            return sb.append(']').toString();
        }

    }

    private boolean checkDuplicates(LinkedHashMap<List<Term>, Integer> ac, LinkedHashMap<List<Term>, Integer> ex, StringBuilder sb) {
        return switch (duplicatesPolicy) {
            case REQUIRE_DEDUP -> {
                var duplicates = ac.keySet().stream().filter(r -> ac.get(r) > 1).toList();
                if (!duplicates.isEmpty()) {
                    sb.append("Rows with duplicates:\n");
                    for (List<Term> row : duplicates) {
                        sb.append("  (").append(ac.get(row)).append(" duplicates)")
                                        .append(toString(row)).append('\n');
                    }
                }
                yield duplicates.isEmpty();
            }
            case ALLOW_DEDUP -> {
                var bad = ac.keySet().stream()
                        .filter(r -> ac.get(r) > ex.getOrDefault(r, 0)).toList();
                if (!bad.isEmpty()) {
                    sb.append("Rows with more duplicates than allowed:\n");
                    for (List<Term> row : bad) {
                        sb.append("  (").append(ac.get(row)).append(" instances, expected ")
                                .append(ex.getOrDefault(row, 0)).append(") ")
                                .append(toString(row)).append('\n');
                    }
                }
                yield bad.isEmpty();
            }
            case ALLOW_MORE_DUPLICATES -> true;
            case EXACT -> {
                var bad = ac.keySet().stream()
                        .filter(r -> !Objects.equals(ac.get(r), ex.getOrDefault(r, 0))).toList();
                if (!bad.isEmpty()) {
                    sb.append("Rows with unexpected number of instances:\n");
                    for (List<Term> row : bad) {
                        sb.append("  (").append(ac.get(row)).append(", expected ")
                                .append(ex.getOrDefault(row, 0)).append(") ")
                                .append(toString(row)).append('\n');
                    }
                }
                yield bad.isEmpty();
            }
        };
    }

    private static boolean checkMissing(LinkedHashMap<List<Term>, Integer> ac,
                                        LinkedHashMap<List<Term>, Integer> ex, StringBuilder sb) {
        var missing = ex.keySet().stream().filter(r -> !ac.containsKey(r)).toList();
        boolean ok = missing.isEmpty();
        if (!ok) {
            sb.append("Missing rows:\n");
            for (List<Term> row : missing)
                sb.append("  ").append(toString(row)).append('\n');
        }
        return ok;
    }

    private static boolean checkUnexpected(LinkedHashMap<List<Term>, Integer> ac,
                                           LinkedHashMap<List<Term>, Integer> ex, StringBuilder sb) {
        var unexpected = ac.keySet().stream().filter(r -> !ex.containsKey(r)).toList();
        boolean ok = unexpected.isEmpty();
        if (!ok) {
            sb.append("Unexpected rows:\n");
            for (List<Term> row : unexpected)
                sb.append("  ").append(toString(row)).append('\n');
        }
        return ok;
    }

    private static void print(Writer w, int indent, Throwable t) throws IOException {
        for (int i = 0; i < indent; i++)
            w.append(' ');
        w.append(t.toString()).append('\n');
        var trace = t.getStackTrace();
        if (trace != null) {
            indent += 2;
            for (var e : trace) {
                for (int i = 0; i < indent; i++) w.append(' ');
                w.append(" at ").append(e.toString()).append('\n');
            }
            indent -= 2;
        }
        Throwable cause = t.getCause();
        if (cause != null) {
            for (int i = 0; i < indent; i++) w.append(' ');
            w.append("Caused by\n");
            print(w, indent+2, cause);
        }
    }

    private boolean checkException(Throwable thrown, StringBuilder sb) {
        String trace;
        if (thrown == null) {
            trace = null;
        } else {
            if (thrown instanceof BItReadFailedException f && f.getCause() != null)
                thrown = f.getCause();
            var bo = new ByteArrayOutputStream();
            try (var w = new PrintWriter(bo)) {
                print(w, 0, thrown);
            } catch (IOException e) {
                log.error("Unexpected IOException writing traces", e);
            }
            trace = bo.toString(UTF_8);
        }
        boolean ok = true;
        if (expectedError == null) {
            if (thrown != null) {
                ok = false;
                sb.append("Unexpected exception thrown: ").append(trace);
            }
        } else {
            if (thrown == null) {
                ok = false;
                sb.append("Expected ").append(expectedError.getSimpleName())
                        .append(" but nothing was thrown");
            } else if (!expectedError.isAssignableFrom(thrown.getClass())) {
                ok = false;
                sb.append("Expected ").append(expectedError.getSimpleName())
                        .append(", but instead got ").append(trace);
            }
        }
        return ok;
    }

    private boolean checkVars(Vars actual, StringBuilder sb) {
        if (vars == null) {
            if (columns != -1 && actual.size() != columns) {
                sb.append("Expected ").append(columns).append(" vars, got ")
                  .append(actual.size()).append(": ").append(actual).append('\n');
                return false;
            }
        } else if (!actual.equals(vars)) {
            sb.append("Expected vars ").append(vars).append(", got ").append(actual);
            return false;
        }
        return true;
    }

    private void count(LinkedHashMap<List<Term>, Integer> map, List<List<Term>> list) {
        for (var row : list)
            map.put(row, map.getOrDefault(row, 0)+1);
    }
}
