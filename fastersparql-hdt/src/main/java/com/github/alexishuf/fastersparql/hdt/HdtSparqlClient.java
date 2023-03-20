package com.github.alexishuf.fastersparql.hdt;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.lang.String.format;

public class HdtSparqlClient extends AbstractSparqlClient {
    private static final Logger log = LoggerFactory.getLogger(HdtSparqlClient.class);

    private final HDT hdt;

    public HdtSparqlClient(SparqlEndpoint ep) {
        super(ep);
        if (ep.protocol() != Protocol.FILE)
            throw new FSInvalidArgument("HdtSparqlClient requires file:// endpoint");
        var path = endpoint.asFile().getAbsolutePath();
        var listener = new LogProgressListener(path);
        try {
            this.hdt = HDTManager.mapIndexedHDT(path, listener);
        } catch (Throwable t) {
            listener.complete(t);
            throw FSException.wrap(ep, t);
        }
    }

    @Override public <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql) {
        throw new UnsupportedOperationException();
    }


    @Override public void close() {
        try {
            hdt.close();
        } catch (IOException e) {
            log.error("Ignoring failure to close HDT object for {}", endpoint);
        }
    }

    /* --- --- --- inner classes --- --- --- */

    private static final class LogProgressListener implements ProgressListener {
        private final String path;
        private final long interval = FSHdtProperties.mapProgressIntervalMs()*1_000_000L;
        private long lastMessage = System.nanoTime();
        private boolean logged;

        public LogProgressListener(String path) {
            this.path = path;
        }

        @Override public void notifyProgress(float level, String message) {
            long now = System.nanoTime();
            long elapsed = now - lastMessage;
            if (elapsed > interval) {
                lastMessage = now;
                log.info("mapping/indexing {}: {}%...", path, format("%.2f", level));
                logged = true;
            }
        }

        public void complete(@Nullable Throwable error) {
            if (error != null)
                log.error("failed to map/index {}", path, error);
            else if (logged)
                log.info("Mapped/indexed {}", path);
        }
    }
}
