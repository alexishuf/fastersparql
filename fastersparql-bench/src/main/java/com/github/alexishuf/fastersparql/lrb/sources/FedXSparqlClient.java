package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.UnitaryBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.async.BItEmitter;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.federated.repository.FedXRepositoryConnection;
import org.eclipse.rdf4j.federated.structures.QueryInfo;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.parser.QueryParserRegistry;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParserFactory;
import org.eclipse.rdf4j.rio.ntriples.NTriplesWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.*;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.*;

public class FedXSparqlClient extends AbstractSparqlClient {
    private static final Logger log = LoggerFactory.getLogger(FedXSparqlClient.class);

    static {
        QueryParserRegistry.getInstance().add(new SPARQLParserFactory());
    }

    private final FederationHandle fedHandle;
    private final FedXRepository fedX;
    private final LIFOPool<ConnState> connPool;

    private final class ConnState implements AutoCloseable {
        public static final int BYTES = 16 /*obj header*/ + 8 /*fields*/
                                      + 92 /*conn*/ + 52 /* map headers & fields */
                                      + 128*4 /*gross simplification*/;
        private final FedXRepositoryConnection conn;
        private final LinkedHashMap<SegmentRope, TupleQuery> planCache
                = new LinkedHashMap<>((int)Math.ceil(128/0.75));

        public ConnState(FedXRepositoryConnection conn) {
            this.conn = conn;
        }

        @Override public void close() {
            try {
                conn.close();
            } catch (Throwable t) {
                log.info("FedXRepositoryConnection to {} did not close cleanly", endpoint, t);
            }
        }

        public TupleQuery plan(SegmentRope sparql) {
            var plan = planCache.getOrDefault(sparql, null);
            if (plan == null) {
                plan = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql.toString());
                if (planCache.size() >= 128) { // remove oldest entry
                    var it = planCache.entrySet().iterator();
                    it.next();
                    it.remove();
                }
                planCache.put(asFinal(sparql), plan);
            }
            return plan;
        }
    }

    public FedXSparqlClient(SparqlEndpoint ep, FederationHandle fedHandle, FedXRepository fedX) {
        super(ep);
        this.fedHandle = fedHandle;
        this.fedX = fedX;
        this.connPool = new LIFOPool<>(ConnState.class, ep.uri(), Alloc.THREADS, ConnState.BYTES);
    }

    @Override protected void doClose() {
        int sleepMs = 0;
        for (QueryInfo q : fedX.getQueryManager().getRunningQueries()) {
            sleepMs = 500;
            try {
                q.abort();
            } catch (Throwable t) { log.info("{} while aborting FedX query", t.getClass()); }
        }
        Async.uninterruptibleSleep(sleepMs);
        sleepMs = 0;
        for (QueryInfo q : fedX.getQueryManager().getRunningQueries()) {
            sleepMs += 500;
            try {
                q.close();
            } catch (Throwable t) { log.info("{} while closing FedX query", t.getClass()); }
        }
        Async.uninterruptibleSleep(sleepMs);
        closeConnections();
        try {
            fedX.shutDown();
        } catch (Throwable t) {
            log.error("fedX shutdown failed for {}: ", endpoint, t);
        }
        try {
            fedHandle.close();
        } catch (Throwable t) {
            log.error("Closing of federation used by FedX at {} failed: ", endpoint, t);
        }
        closeConnections();
    }

    private void closeConnections() {
        for (ConnState c; (c=connPool.get()) != null; )
            c.close();
    }

    @Override public Guard retain() { return new RefGuard(); }

    @Override
    protected <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    doEmit(BatchType<B> bt, SparqlQuery sparql, Vars rebindHint) {
        return BItEmitter.create(doQuery(bt, sparql));
    }

    @Override protected <B extends Batch<B>> BIt<B> doQuery(BatchType<B> bt, SparqlQuery sparql) {
        var conn = connPool.get();
        if (conn == null)
            conn = new ConnState(fedX.getConnection());
        try {
            TupleQuery plan = conn.plan(sparql.sparql());
            plan.setMaxExecutionTime(0);
            return new FedXBIt<>(bt, sparql.publicVars(), conn, plan);
        } catch (Throwable t) {
            conn.close();
            throw t;
        }
    }

    private class FedXBIt<B extends Batch<B>> extends UnitaryBIt<B> {
        private static final FinalSegmentRope ANON_BNODE_PREFIX = asFinal("_:fedx");
        private static final FinalSegmentRope BNODE_PREFIX      = asFinal("_:");
        private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

        private ConnState connState;
        private final TupleQuery query;
        private @MonotonicNonNull TupleQueryResult result;
        private List<String> varsStrings;
        private final MutableRope tmp = new MutableRope(64);
        private final NtHelper ntHelper = new NtHelper(tmp);
        private int nextBNodeId = 1;


        public FedXBIt(BatchType<B> batchType, Vars vars, ConnState connState, TupleQuery query) {
            super(batchType, vars);
            this.connState = connState;
            this.query     = query;
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            try {
                super.cleanup(cause);
            } finally {
                tmp.close();
                if (connState != null) {
                    connPool.offer(connState);
                    connState = null;
                }
            }
        }

        @Override public boolean tryCancel() {
            if (!state().isTerminated() && result != null) {
                try {
                    result.close();
                } catch (Throwable t) { log.info("Failed to cancel {}", this, t); }
            }
            return super.tryCancel();
        }

        @Override protected B fetch(B dest) {
            if (result == null) {
                result = query.evaluate();
                varsStrings = result.getBindingNames();
            }
            if (result.hasNext()) {
                dest.beginPut();
                BindingSet row = result.next();
                for (int col = 0, n = varsStrings.size(); col < n; col++) {
                    var name = varsStrings.get(col);
                    var value = row.getValue(name);
                    switch (value) {
                        case Literal lit -> {
                            tmp.clear();
                            FinalSegmentRope shared;
                            if (needsLiteralSuffix(lit.getCoreDatatype())) {
                                shared = dtSuffix(lit, tmp);
                                ntHelper.write(SVF.createLiteral(lit.getLabel()));
                                if (shared.len > 0 && tmp.len > 0)
                                    --tmp.len; // erase closing ", already present in shared
                            } else {
                                shared = EMPTY;
                                ntHelper.write(lit);
                            }
                            dest.putTerm(col, shared, tmp, 0, tmp.len, true);
                        }
                        case BNode bn -> {
                            String id = bn.getID();
                            if (id == null)
                                tmp.append(ANON_BNODE_PREFIX).append(nextBNodeId++);
                            else
                                tmp.append(BNODE_PREFIX).append(id);
                            dest.putTerm(col, EMPTY, tmp, 0, tmp.len, false);
                        }
                        case IRI iri -> {
                            tmp.clear().append('<').append(iri.getNamespace());
                            var shared = SHARED_ROPES.internPrefix(tmp, 0, tmp.len);
                            tmp.clear().append(iri.getLocalName()).append('>');
                            dest.putTerm(col, shared, tmp, 0, tmp.len, false);
                        }
                        case null -> { /* do nothing */ }
                        default -> throw new InvalidSparqlResultsException("unexpected RDF value type: "+value.getClass());
                    }
                }
                dest.commitPut();
            } else {
                exhausted = true;
            }
            return dest;
        }
    }

    private static boolean needsLiteralSuffix(CoreDatatype dt) {
        return switch (dt) {
            case CoreDatatype.XSD xsd -> xsd != CoreDatatype.XSD.STRING;
            case CoreDatatype.RDF rdf -> rdf != CoreDatatype.RDF.LANGSTRING;
            default                   -> true;
        };
    }

    private static FinalSegmentRope dtSuffix(Literal lit, MutableRope tmp)  {
        FinalSegmentRope suff = switch (lit.getCoreDatatype()) {
            case CoreDatatype.XSD xsd -> switch (xsd) {
                case ENTITIES, ENTITY, ID, IDREF, IDREFS, NCNAME, NMTOKEN,
                     NMTOKENS, NAME, QNAME, NOTATION, DATETIMESTAMP, YEARMONTHDURATION,
                     DAYTIMEDURATION -> null;
                case ANYURI -> DT_anyURI ;
                case BASE64BINARY -> DT_base64Binary ;
                case BOOLEAN -> DT_BOOLEAN ;
                case BYTE -> DT_BYTE ;
                case DATE -> DT_date ;
                case DATETIME -> DT_dateTime ;
                case DECIMAL -> DT_decimal ;
                case DOUBLE -> DT_DOUBLE ;
                case DURATION -> DT_duration ;
                case FLOAT -> DT_FLOAT ;
                case GDAY -> DT_gDay ;
                case GMONTH -> DT_gMonth ;
                case GMONTHDAY -> DT_gMonthDay ;
                case GYEAR -> DT_gYear ;
                case GYEARMONTH -> DT_gYearMonth ;
                case HEXBINARY -> DT_hexBinary ;
                case INT -> DT_INT ;
                case INTEGER -> DT_integer ;
                case LANGUAGE -> DT_language ;
                case LONG -> DT_LONG ;
                case NEGATIVE_INTEGER -> DT_negativeInteger ;
                case NON_NEGATIVE_INTEGER -> DT_nonNegativeInteger ;
                case NON_POSITIVE_INTEGER -> DT_nonPositiveInteger ;
                case NORMALIZEDSTRING -> DT_normalizedString ;
                case POSITIVE_INTEGER -> DT_positiveInteger ;
                case SHORT -> DT_SHORT ;
                case STRING -> DT_string ;
                case TIME -> DT_time ;
                case TOKEN -> DT_token ;
                case UNSIGNED_BYTE -> DT_unsignedByte ;
                case UNSIGNED_INT -> DT_unsignedInt ;
                case UNSIGNED_LONG -> DT_unsignedLong ;
                case UNSIGNED_SHORT -> DT_unsignedShort ;
            };
            case CoreDatatype.RDF rdf -> switch (rdf) {
                case HTML -> DT_HTML;
                case LANGSTRING -> EMPTY;
                case XMLLITERAL -> DT_XMLLiteral;
            };
            case null -> EMPTY;
            default -> null;
        };
        if (suff != null)
            return suff;
        IRI dt = lit.getDatatype();
        int begin = tmp.len;
        tmp.append(DT_MID_LT).append(dt.getNamespace()).append(dt.getLocalName()).append('>');
        FinalSegmentRope interned = SHARED_ROPES.internDatatype(tmp, begin, tmp.len);
        tmp.len = begin;
        return interned;
    }

    private static final class NtHelper extends NTriplesWriter {
        public NtHelper(MutableRope tmp) {
            super(new OutputStreamWriter(tmp.asOutputStream(), StandardCharsets.UTF_8));
        }
        public void write(Value value) {
            try {
                writeValue(value);
                writer.flush();
            } catch (IOException e) {
                throw new RuntimeException(e); // impossible
            }
        }
    }

}
