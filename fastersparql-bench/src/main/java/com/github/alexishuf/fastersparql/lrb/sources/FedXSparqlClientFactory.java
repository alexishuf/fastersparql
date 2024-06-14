package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.fed.Source;
import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.endpoint.Endpoint;
import org.eclipse.rdf4j.federated.endpoint.EndpointFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FedXSparqlClientFactory implements SparqlClientFactory {
    private static final Logger log = LoggerFactory.getLogger(FedXSparqlClientFactory.class);

    @Override public String tag() {return "fedx";}
    @Override public int order() {return 10;}

    @Override public boolean supports(SparqlEndpoint endpoint) {
        String uri = endpoint.uri();
        Matcher matcher = KIND_RX.matcher(uri);
        if (matcher.matches())
            return true;
        else if (uri.toLowerCase().contains("fedx"))
            log.debug("refusing support of {}: does not match {}", uri, KIND_RX);
        return false;
    }

    private static final Pattern KIND_RX = Pattern.compile("process://fedx/(.*)/\\?dir=(.*)");

    @Override public SparqlClient createFor(SparqlEndpoint endpoint) {
        Matcher matcher = KIND_RX.matcher(endpoint.uri());
        if (!matcher.find())
            throw new FSException("Unsupported uri: "+endpoint.uri());
        SourceKind srcKind = SourceKind.valueOf(matcher.group(1));
        Path dataDir = Path.of(matcher.group(2));
        if (!Files.isDirectory(dataDir))
            throw new FSException("Path "+dataDir+" is nto a directory");
        if (!srcKind.isServer() || srcKind.isWs()) {
            throw new FSException("FedX requires HTTP (non-WebSocket) servers as sources. "
                                  +srcKind+" is not allowed");
        }
        FederationHandle fedHandle = null;
        try {
            Path fxDir = dataDir.resolve("FedX");
            Files.createDirectories(fxDir);
            fedHandle = FederationHandle.builder(dataDir.toFile()).srcKind(srcKind).subset(LrbSource.all())
                    .selKind(SelectorKind.ASK).waitInit(true).create();
            List<Endpoint> endpoints = new ArrayList<>();
            fedHandle.federation.forEachSource((src, callback) -> {
                String name = src.spec().getString(Source.NAME);
                String uri = src.client.endpoint().uri();
                endpoints.add(EndpointFactory.loadSPARQLEndpoint(name, uri));
                callback.apply(null, null); // done
            });
            var fx = FedXFactory.newFederation()
                    .withConfig(new FedXConfig().withEnforceMaxQueryTime(0))
                    .withFedXBaseDir(fxDir.toFile())
                    .withMembers(endpoints).create();
            var client = new FedXSparqlClient(endpoint, fedHandle, fx);
            fedHandle = null;
            return client;
        } catch (Throwable t) {
            var fe = t instanceof FSException fse ? fse
                            : new FSException("Failed to create a FedXSparqlClient", t);
            fe.endpoint(endpoint);
            throw fe;
        } finally {
            if (fedHandle != null)
                fedHandle.close();
        }
    }
}
