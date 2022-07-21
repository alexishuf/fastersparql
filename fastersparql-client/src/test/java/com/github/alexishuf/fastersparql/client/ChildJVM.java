package com.github.alexishuf.fastersparql.client;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.ProcessBuilder.Redirect.PIPE;
import static java.util.Arrays.asList;

@Accessors(fluent = true)
public class ChildJVM implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ChildJVM.class);
    @Getter private final Process process;

    @Builder
    public ChildJVM(Class<?> mainClass, @Singular Map<String, String> envVars,
                    ProcessBuilder.Redirect errorRedirect,
                    ProcessBuilder.Redirect outputRedirect,
                    boolean redirectErrorStream,
                    @Singular List<String> jvmArgs,
                    @Singular List<String> mainArgs) throws IOException {
        if (mainClass == null)
            throw new IllegalArgumentException("mainClass is required");
        String separator = System.getProperty("file.separator");
        String suffix = separator.equals("\\") ? ".exe" : "";
        String home = System.getProperty("java.home");
        if (home == null)
            throw new RuntimeException("java.home property not set, cannot find java executable");
        File java = new File(home + separator + "bin" + separator + "java" + suffix);
        if (!java.exists())
            throw new RuntimeException("Java executable does not exist at "+java);
        List<String> command = new ArrayList<>(asList(
                java.getAbsolutePath(), "-cp", System.getProperty("java.class.path")));
        command.addAll(jvmArgs);
        command.add(mainClass.getName());
        command.addAll(mainArgs);
        ProcessBuilder builder = new ProcessBuilder(command)
                .redirectError(errorRedirect == null ? PIPE : errorRedirect)
                .redirectOutput(outputRedirect == null ? PIPE : outputRedirect)
                .redirectErrorStream(redirectErrorStream);
        builder.environment().putAll(envVars);
        this.process = builder.start();
    }

    private String readAll(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder builder = new StringBuilder();
        for (String l = reader.readLine(); l != null; l = reader.readLine()) {
            builder.append(l).append('\n');
        }
        return builder.toString();
    }

    public String readAllOutput() throws IOException {
        return readAll(process.getInputStream());
    }

    public String readAllError() throws IOException {
        return readAll(process.getErrorStream());
    }

    @Override public void close() throws Exception {
        close(0, 1000, 1000);
    }

    public void close(int waitMs, int destroyWaitMs,
                      int forceWaitMs) throws Exception {
        if (process.isAlive()) {
            if (!process.waitFor(waitMs, TimeUnit.MILLISECONDS)) {
                process.destroy();
                if (!process.waitFor(destroyWaitMs, TimeUnit.MILLISECONDS)) {
                    process.destroyForcibly();
                    if (!process.waitFor(forceWaitMs, TimeUnit.MILLISECONDS)) {
                        log.error("close(): Leaking Process {} after {}ms and destroyForcibly()",
                                  process, waitMs+destroyWaitMs+forceWaitMs);
                    }
                }
            }
        }
    }
}
