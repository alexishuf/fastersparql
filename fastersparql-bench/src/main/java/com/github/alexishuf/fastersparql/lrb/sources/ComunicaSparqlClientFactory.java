package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.alexishuf.fastersparql.lrb.sources.ServerProcess.createSparqlClient;

public class ComunicaSparqlClientFactory implements SparqlClientFactory {
    private static final Logger log = LoggerFactory.getLogger(ComunicaSparqlClientFactory.class);

    @Override public String toString() {
        return getClass().getSimpleName()+'@'+Integer.toHexString(System.identityHashCode(this));
    }

    @Override public String   tag() { return "comunica"; }
    @Override public int    order() { return 10; }


    private static final SparqlConfiguration SUPPORTED_CONFIG = SparqlConfiguration.builder()
            .methods(List.of(SparqlMethod.GET, SparqlMethod.POST))
            .resultsAccepts(List.of(SparqlResultFormat.TSV, SparqlResultFormat.JSON))
            .build();

    @Override public boolean supports(SparqlEndpoint endpoint) {
        String uri = endpoint.uri();
        if (FILE_RX.matcher(uri).find()) {
            if (endpoint.configuration().isAcceptedBy(SUPPORTED_CONFIG))
                return true;
            log.warn("refusing support of {}: methods/accept are not supported",
                     endpoint.augmentedUri());
            return false;
        } else if (uri.contains("comunica")) {
            log.debug("refusing support of {}: does not match {}", uri, FILE_RX);
        }
        return false;
    }

    @Override public SparqlClient createFor(SparqlEndpoint endpoint) {
        Matcher m = FILE_RX.matcher(endpoint.uri());
        if (!m.find())
            throw new FSException("Unsupported uri: "+endpoint.uri());
        var path = Path.of(m.group(1));
        if (!Files.isRegularFile(path) || !Files.isReadable(path))
            throw new FSException("File "+path+" is a dir, does not exist or is not readable");
        var dir = path.getParent();
        if (dir == null)
            dir = Path.of("").toAbsolutePath();

        List<String> cmdLine = new ArrayList<>();
        cmdLine.add("comunica-sparql-hdt-http");
        cmdLine.add("-i"); // do not cache queries
        int port = ServerProcess.freePort(endpoint);
        cmdLine.add("--port");
        cmdLine.add(String.valueOf(port));
        String name;
        if (path.getFileName().toString().endsWith(".list")) {
            name = "comunica-fed";
            for (String file : parseListsFile(path))
                cmdLine.add("hdt@"+file);
        } else {
            name = "comunica-"+path.getFileName();
            cmdLine.add("hdt@"+path.getFileName());
        }
        var builder = new ProcessBuilder().command(cmdLine).directory(dir.toFile());
        return createSparqlClient(builder, name, endpoint, port, "/sparql");
    }
    private static final Pattern FILE_RX = Pattern.compile("process://comunica/\\?file=(.*)$");

    private static List<String> parseListsFile(Path listFile) {
        List<String> relativeFiles = new ArrayList<>();
        listFile = listFile.toAbsolutePath();
        Path dir = listFile.getParent();
        try (var reader = new BufferedReader(new FileReader(listFile.toFile()))) {
            for (String line; (line=reader.readLine()) != null; ) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                Path relative = Path.of(line);
                Path resolved = dir.resolve(relative);
                if (Files.exists(resolved)) {
                    relativeFiles.add(line);
                } else {
                    var sb = new StringBuilder().append("File").append(line);
                    sb.append(" does not exist");
                    if (!relative.isAbsolute())
                        sb.append(" (relative to ").append(listFile).append(')');
                    throw new IOException(sb.toString());
                }
            }
            return relativeFiles;
        } catch (IOException e) {
            throw new FSException("Could not read "+listFile+": "+e.getMessage());
        }
    }
}
