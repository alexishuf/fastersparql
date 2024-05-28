package com.github.alexishuf.fastersparql.lrb.sources;

import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.util.SafeCloseable;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.time.Duration;
import java.util.List;

import static com.github.alexishuf.fastersparql.util.concurrent.Async.uninterruptibleWaitFor;
import static java.util.concurrent.TimeUnit.*;

public class ServerProcess implements SafeCloseable {
    private static final Logger log = LoggerFactory.getLogger(ServerProcess.class);
    private static final boolean LOG_FILE = ServerProcess.class.desiredAssertionStatus();
    private static boolean loggedLogFileDisabled = false;

    private boolean alive;
    private final List<String> commandLine;
    private final String name;
    private final SparqlEndpoint httpEndpoint;
    private final int port;
    private @Nullable String commandLineString;
    private @Nullable String deadString;
    public final Process process;

    public ServerProcess(ProcessBuilder processBuilder, String name,
                         SparqlConfiguration clientConfig,
                         int port, String sparqlPath) throws IOException {
        var dir = processBuilder.directory();
        var logFileName = name + ".log";
        var logFile = dir == null ? new File(logFileName) : new File(dir, logFileName);
        processBuilder.redirectErrorStream(true);
        if (LOG_FILE) {
            processBuilder.redirectOutput(logFile);
        } else {
            processBuilder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
            if (!loggedLogFileDisabled) {
                loggedLogFileDisabled = true;
                log.info("""
                        stdout and stderr are closed/redirected to null. \
                        Run the benchmark/tests with assertions enabled to save output \
                        to disk. This message will not repeat.""");
            }
        }
        this.name = name;
        this.port = port;
        this.commandLine = processBuilder.command();
        var httpUri = "http://127.0.0.1:" + port
                    + (sparqlPath.startsWith("/") ? "" : "/") + sparqlPath;
        this.httpEndpoint = new SparqlEndpoint(httpUri, clientConfig);
        this.process = processBuilder.start();
        this.alive = true;
        process.onExit().whenComplete((ignored1, ignored2) -> alive = false);
    }

    public static int freePort(@Nullable SparqlEndpoint processEndpoint) {
        int port;
        try (var server = new ServerSocket()) {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress((InetAddress)null, 0));
            port = server.getLocalPort();
        } catch (IOException e) {
            throw new FSException(processEndpoint, "Could not find a free TCP port");
        }
        return port;
    }

    public boolean waitForPort(Duration duration) {
        long now = System.nanoTime();
        long logAt = now + NANOSECONDS.convert(1, SECONDS);
        long deadline = now + duration.toNanos();
        InetAddress localhost;
        try {
            localhost = InetAddress.getByAddress("localhost", LOCALHOST_IPv4);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e); // impossible
        }
        var address = new InetSocketAddress(localhost, port);
        do {
            if (now > logAt) {
                log.info("Waiting for port {} to be served by {}", port, process);
                logAt = Long.MAX_VALUE;
            }
            try (var s = new Socket()) {
                Async.uninterruptibleSleep(10);
                s.setSoLinger(true, 0);
                int tout = (int) Math.min(MILLISECONDS.convert(deadline-now, NANOSECONDS), 500);
                s.setSoTimeout(tout);
                s.connect(address, tout);
                return true;
            } catch (IOException ignored) {}
        } while ((now=System.nanoTime()) < deadline && process.isAlive());
        return false;
    }
    private static final byte[] LOCALHOST_IPv4 = {127, 0, 0, 1};

    public String commandLineString() {
        if (commandLineString == null) {
            var sb = new StringBuilder();
            for (String arg : commandLine) {
                if (arg.contains(" "))
                    sb.append('"').append(arg.replace("\"", "\\\"")).append('"');
                else
                    sb.append(arg);
                sb.append(' ');
            }
            sb.setLength(Math.max(0, sb.length()-1));
            commandLineString = sb.toString();
        }
        return commandLineString;
    }

    public SparqlEndpoint httpEndpoint() { return httpEndpoint; }
    public Process             process() { return process; }
    public String                 name() { return name; }
    public boolean             isAlive() { return alive; }
    public boolean              isDead() { return !alive; }

    public FSServerException makeDeadException(SparqlEndpoint endpoint) {
        if (deadString == null)
            deadString = "Server died, command-line: "+commandLineString();
        throw new FSServerException(endpoint, deadString);
    }

    @Override public void close() {
        for (int i = 0; i < 4; ++i) {
            if (i < 2) process.destroy();
            else       process.destroyForcibly();
            if (uninterruptibleWaitFor(process, 2, SECONDS))
                return;
        }
        log.error("Process PID={} refuses to exit, leaking. Command line: {}",
                  process.pid(), commandLineString());

    }

    @Override public String toString() {
        var sb = new StringBuilder();
        sb.append("ServerProcess[");
        if (alive)
            sb.append("ALIVE");
        else
            sb.append("exit=").append(process.exitValue());
        sb.append("]{pid=").append(process.pid()).append(", cmd=");

        String cmd = commandLine.isEmpty() ? "<unknown command>" : commandLine.getFirst();
        if (cmd.length() < 32)
            sb.append(cmd);
        else
             sb.append("...").append(cmd, cmd.length()-28, cmd.length());
        return sb.append('}').toString();
    }
}
