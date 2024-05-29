package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import org.apache.commons.lang3.SystemUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.channels.Channels.newWriter;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.*;

public class VirtuosoSparqlClientFactory implements SparqlClientFactory {
    private static final Logger log = LoggerFactory.getLogger(VirtuosoSparqlClientFactory.class);

    @Override public String toString() {
        return getClass().getSimpleName()+'@'+Integer.toHexString(System.identityHashCode(this));
    }

    @Override public String   tag() { return "virtuoso"; }
    @Override public int    order() { return 10; }

    @Override public boolean supports(SparqlEndpoint endpoint) {
        Matcher m = FILE_RX.matcher(endpoint.uri());
        if (!m.find())
            return false;
        if (SystemUtils.IS_OS_LINUX)
            return true;
        log.warn("Spawning a virtuoso process from {} is only supported on 64bit linux",
                 m.group(1));
        return false;
    }
    private static final Pattern FILE_RX = Pattern.compile("process://virtuoso/\\?file=(.*)$");

    @Override public SparqlClient createFor(SparqlEndpoint endpoint) {
        Matcher fileMatcher = FILE_RX.matcher(endpoint.uri());
        if (!fileMatcher.find())
            throw new FSException("Unsupported uri: "+endpoint.uri());
        Path dir = Path.of(fileMatcher.group(1));
        Path bin = dir.resolve("bin"), executable = bin.resolve("virtuoso-t");
        if (!Files.isRegularFile(executable) || !Files.isExecutable(executable))
            throw new FSException(executable+" is not an executable file");
        int sparqlPort = makeIniFileAndGetSparqlPort(dir);
        try {
            var cmd = List.of("./virtuoso-t", "-fd", "-c", "../var/lib/virtuoso/db/fs-virtuoso.ini");
            var builder = new ProcessBuilder(cmd).directory(bin.toFile());
            var ldLibraryPath = ldLibraryPath();
            if (ldLibraryPath != null)
                builder.environment().put("LD_LIBRARY_PATH", ldLibraryPath);
            var name = dir.getFileName().toString().replace("-7.1-64bit-linux", "");
            var proc = new ServerProcess(builder,
                    name, endpoint.configuration(),
                    sparqlPort, "/sparql");
            if (!proc.waitForPort(PORT_TIMEOUT)) {
                if (proc.isAlive()) {
                    log.error("{} is not listening at port {} after {}s, killing",
                              proc, sparqlPort, PORT_TIMEOUT.toSeconds());
                }
                proc.close();
                throw proc.makeDeadException(endpoint);
            }
            return new ProcessNettySparqlClient(proc.httpEndpoint(), proc);
        } catch (Throwable t) {
            if (t instanceof FSException e) {
                e.endpoint(endpoint);
                throw e;
            }
            throw new FSException(endpoint, "Failed to start process", t);
        }
    }
    private static final Duration PORT_TIMEOUT = Duration.ofSeconds(60);

    private static @Nullable String ldLibraryPath() {
        String ldPath = System.getenv("LD_LIBRARY_PATH");
        String virtuosoLDPath = System.getenv("FS_VIRTUOSO_LD_LIBRARY_PATH");
        if (virtuosoLDPath == null || (virtuosoLDPath=virtuosoLDPath.trim()).isEmpty())
            virtuosoLDPath = System.getProperty("fastersparql.virtuoso.ld_library_path");
        if (virtuosoLDPath != null && !(virtuosoLDPath=virtuosoLDPath.trim()).isEmpty()) {
            String merged = virtuosoLDPath;
            if (ldPath != null && !(ldPath=ldPath.trim()).isEmpty())
                merged = virtuosoLDPath+":"+ldPath;
            return merged;
        } else {
            return null;
        }
    }

    /**
     * Parse the {@code var/lib/virtuoso/db/virtuoso.ini} file to extract the SPARQL server port
     * and generate a second ini file, {@code fs-virtuoso.ini}, at same dir that is a copy of the
     * original, except it changes limits on CPU and memory usage to fit (and fill) current free
     * int the host machine.
     */
    private static int makeIniFileAndGetSparqlPort(Path virtuosoDir) {
        Path in = virtuosoDir.resolve("var/lib/virtuoso/db/virtuoso.ini").toAbsolutePath();
        Path out = in.resolveSibling("fs-virtuoso.ini");
        int sparqlPort = -1;
        boolean atHTTPServer = false, atParameters = false;
        try {
            List<String> lines = Files.readAllLines(in, UTF_8);
            for (int i = 0, linesSize = lines.size(); i < linesSize; i++) {
                String line = lines.get(i);
                Matcher matcher;
                if (line.contains("[Parameters]") && line.trim().startsWith("[")) {
                    atParameters = true;
                    lines.set(i, String.format("""
                                    %s
                                    NumberOfBuffers      = %d
                                    MaxDirtyBuffers      = %d
                                    MaxQueryMem          = %s
                                    ThreadsPerQuery      = %d
                                    AsyncQueueMaxThreads = %d""",
                            line, NUMBER_OF_BUFFERS, MAX_DIRTY_BUFFERS,
                            MAX_QUERY_MEM, THREADS_PER_QUERY, ASYNC_QUEUE_MAX_THREADS));
                } else if (atParameters && SERVER_PORT.matcher(line).find()) {
                    // at least LTCGA-E and Affymetrix use the same port (1124)
                    lines.set(i, "ServerPort = "+ServerProcess.freePort(null));
                } else if (line.contains("[SPARQL]")) {
                    lines.set(i, line+"\nMaxQueryExecutionTime = "+MAX_QUERY_EXECUTION_TIME);
                } else if ((matcher=MUST_COMMENT.matcher(line)).find()) {
                    lines.set(i, matcher.replaceFirst(";$0"));
                } else if (line.contains("[HTTPServer]") && !line.trim().startsWith(";")) {
                    atHTTPServer = true;
                    atParameters = false;
                } else if (atHTTPServer && (matcher=SERVER_PORT.matcher(line)).find()) {
                    try {
                        sparqlPort = Integer.parseInt(matcher.group(1));
                    } catch (NumberFormatException e) {
                        throw new FSException("ServerPort value inside [HTTPServer] at "+
                                              in+" is not a number: "+
                                              matcher.group(1));
                    }
                    if (sparqlPort <= 0 || sparqlPort >= 1<<16) {
                        throw new FSException("ServerPort="+sparqlPort+"is not an unsigned " +
                                              "16-bit positive number at "+in);
                    }
                }
            }
            try (var w = newWriter(FileChannel.open(out, TRUNCATE_EXISTING,WRITE,CREATE), UTF_8)) {
                for (String line : lines)
                    w.append(line).append('\n');
            }
        } catch (IOException e) {
            throw new FSException("Could not create "+out+" from "+in, e);
        }
        if (sparqlPort == -1)
            throw new FSException("No [HTTPServer].ServerPort found in "+in);
        return sparqlPort;
    }
    private static final Pattern MUST_COMMENT = Pattern.compile("^\\s*(?:NumberOfBuffers|MaxDirtyBuffers|MaxQueryMem|ThreadsPerQuery|AsyncQueueMaxThreads|MaxMemInUse|MaxQueryExecutionTime)\\s*=");
    private static final Pattern SERVER_PORT = Pattern.compile("^\\s*ServerPort\\s*=\\s*(\\d+)");

    private static final int NUMBER_OF_BUFFERS;
    private static final int MAX_DIRTY_BUFFERS;
    private static final int ASYNC_QUEUE_MAX_THREADS;
    private static final int THREADS_PER_QUERY;
    private static final String MAX_QUERY_MEM;
    private static final int MAX_QUERY_EXECUTION_TIME;
    static {
        var hal        = new SystemInfo().getHardware();
        int cores      = hal.getProcessor().getPhysicalProcessorCount();
        int megabytes  = 1024*1024;
        int mediatorReserved = 2048*megabytes;
        long freeBytes = Math.max(256*megabytes,
                                  (long)(0.8*hal.getMemory().getAvailable()-mediatorReserved));
        NUMBER_OF_BUFFERS        = (int)(freeBytes*0.5/13/8700);
        MAX_DIRTY_BUFFERS        = (int)(0.75*NUMBER_OF_BUFFERS);
        MAX_QUERY_MEM            = freeBytes/13/4/megabytes + "M";
        MAX_QUERY_EXECUTION_TIME = 300; // 5min
        ASYNC_QUEUE_MAX_THREADS  = cores;
        THREADS_PER_QUERY        = Math.max(1, cores/4);
    }
}
