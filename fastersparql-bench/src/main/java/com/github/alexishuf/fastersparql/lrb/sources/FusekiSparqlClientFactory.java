package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FusekiSparqlClientFactory implements SparqlClientFactory {
    private static final Logger log = LoggerFactory.getLogger(FusekiSparqlClientFactory.class);

    @Override public String toString() {return getClass().getSimpleName();}
    @Override public String      tag() {return "fuseki";}
    @Override public int       order() {return 10;}

    @Override public boolean supports(SparqlEndpoint endpoint) {
        return dirFromProcessUri(endpoint.uri()) != UNSUPPORTED;
    }

    private Path dirFromProcessUri(String uri) {
        Matcher matcher = FUSEKI_URI.matcher(uri);
        if (!matcher.find())
            return UNSUPPORTED; // not a process://fuseki URI
        Path dir = Path.of(matcher.group(1));
        if (!Files.isDirectory(dir))
            return UNSUPPORTED;
        try {
            boolean hasGenerations = Files.list(dir).filter(Files::isDirectory)
                    .map(Path::getFileName).map(Path::toString)
                    .anyMatch(name -> name.startsWith("Data-"));
            if (!hasGenerations)
                log.debug("No Data-* dirs in {}: not a TDB2", dir);
            return hasGenerations ? dir : UNSUPPORTED;
        } catch (IOException e) {
            log.error("IO error reading contents of {}, treating as if not a TDB2 dir", dir);
            return UNSUPPORTED;
        }
    }
    private static final Path UNSUPPORTED = Path.of("urn:fastersparql:NOT_A_TDB2_DIR");
    private static final Pattern FUSEKI_URI = Pattern.compile("^process://fuseki/\\?file=(.*)$");

    @Override public SparqlClient createFor(SparqlEndpoint endpoint) {
        Path dir = dirFromProcessUri(endpoint.uri());
        if (dir == UNSUPPORTED)
            throw new FSException(endpoint, "Not a process:// endpoint pointing to a TDB2 dir");
        int port = ServerProcess.freePort(endpoint);
        Path runDir = dir.resolveSibling(dir.getFileName()+".run");
        if (Files.isRegularFile(runDir))
            throw new FSException(endpoint, "runDir="+runDir+" already exists as a file");
        try {
            Files.createDirectories(runDir);
        } catch (IOException e) {
            throw new FSException(endpoint, "Could not create runDir="+runDir);
        }
        var cmd = List.of(fusekiLauncher(endpoint), "--tdb2",
                          "--loc", runDir.resolve(dir.toAbsolutePath()).toString(),
                          "--port", String.valueOf(port), "/ds");
        var builder = new ProcessBuilder().command(cmd).directory(runDir.toFile());
        builder.environment().put("JVM_ARGS", fusekiXmx());
        return ServerProcess.createSparqlClient(builder, dir.getFileName().toString(),
                                                endpoint, port, "/ds/sparql");
    }

    private static String fusekiLauncher(@Nullable SparqlEndpoint errorForEndpoint) {
        if (fusekiLauncher != null && !fusekiLauncher.startsWith(NO_FUSEKI_PREFIX))
            return fusekiLauncher;
        if (fusekiLauncher == null) {
            String launcher = System.getProperty("fastersparql.fuseki");
            if (launcher == null || (launcher = launcher.trim()).isEmpty()) {
                launcher = "fuseki-server"; // default: search in PATH
            } else {
                Path path = Path.of(launcher);
                if (Files.isDirectory(path)) {
                    Path file = path.resolve("fuseki-server");
                    if (Files.isRegularFile(file) && Files.isExecutable(file))
                        launcher = file.toAbsolutePath().toString();
                }
            }
            boolean interrupted = false;
            Path logFile;
            try {
                logFile = Files.createTempFile("fastersparql", ".fuseki.log");
            } catch (IOException e) { logFile = null; }
            try {
                var procBuilder = new ProcessBuilder(launcher, "--version");
                if (logFile == null)
                    procBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                else
                    procBuilder.redirectErrorStream(true).redirectOutput(logFile.toFile());
                var proc = procBuilder.start();
                long deadline = System.nanoTime() + NANOSECONDS.convert(5, SECONDS);
                for (long ns; fusekiLauncher == null && (ns = deadline - System.nanoTime()) > 0; ) {
                    try {
                        if (proc.waitFor(ns, NANOSECONDS)) {
                            if (proc.exitValue() == 0) {
                                fusekiLauncher = launcher;
                            } else {
                                fusekiLauncher = NO_FUSEKI_PREFIX+launcher;
                                var logFileContents = logFile == null ? "<<see above>>"
                                                    : Files.readString(logFile);
                                log.warn("""
                                        {} is in PATH but had non-zero exit status with output:
                                        {}""", launcher, logFileContents);
                            }
                        }
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted)
                    Thread.currentThread().interrupt();
            } catch (IOException e) {
                fusekiLauncher = NO_FUSEKI_PREFIX+launcher;
            } finally {
                try {
                    if (logFile != null) Files.deleteIfExists(logFile);
                } catch (IOException ignored) {}
            }
        }
        if (fusekiLauncher.startsWith(NO_FUSEKI_PREFIX)) {
            if (fusekiCmdMissingMessage == null) {
                var cmd = fusekiLauncher.substring(NO_FUSEKI_PREFIX.length());
                fusekiCmdMissingMessage = cmd+" not in PATH, not executable, corrupted or missing dependencies (e.g., no java in PATH)";
            }
            throw new FSException(errorForEndpoint, fusekiCmdMissingMessage);
        }
        return fusekiLauncher;
    }
    private static String fusekiLauncher = null;
    private static String fusekiCmdMissingMessage = null;
    private static final String NO_FUSEKI_PREFIX = "#missing:";

    private static String fusekiXmx = null;

    private static String fusekiXmx() {
        if (fusekiXmx != null)
            return fusekiXmx; // compute only once, else Xmx will decrease after each call
        long megabytes = 1024*1024;
        // get physical RAM which is not used by RSS of processes
        long freeBytes = new SystemInfo().getHardware().getMemory().getAvailable();
        // the mediator should never use more than 2GiB. If it has been launched with
        // a heap limit below that, use it
        long mediatorBytes = Math.min(2048*megabytes, Runtime.getRuntime().maxMemory());
        long minHeapPerFuseki = 128*megabytes; // avoid OutOfMemoryError
        // Reserve 40% for off-heap memory (Fuseki uses a lot of these) and OS-managed IO caches.
        long heapCapacityOnHost = (long)(0.6*freeBytes);
        // divide all heap capacity equally among all sources
        long heapPartitionBytes = (heapCapacityOnHost - mediatorBytes)/LrbSource.all().size();
        return fusekiXmx = "-Xmx"+Math.max(minHeapPerFuseki, heapPartitionBytes)/megabytes+"m";
    }

}
