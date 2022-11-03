package com.github.alexishuf.fastersparql.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.ProcessBuilder.Redirect.PIPE;
import static java.util.Arrays.asList;

public class ChildJVM implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ChildJVM.class);
    private final Process process;

    @SuppressWarnings("unused")
    public static final class Builder {
        private final Class<?> mainClass;
        private ProcessBuilder.Redirect errorRedirect = PIPE;
        private ProcessBuilder.Redirect outputRedirect = PIPE;
        private boolean redirectErrorStream = false;
        private Map<String, String> envVars = new HashMap<>();
        private List<String> jvmArgs = new ArrayList<>(List.of("--enable-preview"));
        private List<String> mainArgs = new ArrayList<>();

        public Builder(Class<?> mainClass) {
            this.mainClass = mainClass;
        }

        public Builder errorRedirect(ProcessBuilder.Redirect value)  {      errorRedirect = value; return this;}
        public Builder outputRedirect(ProcessBuilder.Redirect value) {     outputRedirect = value; return this;}
        public Builder redirectErrorStream(boolean value)            {redirectErrorStream = value; return this;}
        public Builder envVars(Map<String, String> value)            {            envVars = value; return this;}
        public Builder jvmArgs(List<String> value)                   {            jvmArgs = value; return this;}
        public Builder mainArgs(List<String> value)                  {           mainArgs = value; return this;}

        public Builder envVar(String name, String value) {envVars.put(name, value); return this;}
        public Builder jvmArg(String arg)             {jvmArgs.add(arg); return this;}
        public Builder mainArg(String arg)            {mainArgs.add(arg); return this;}

        public ChildJVM build() throws IOException {
            return new ChildJVM(mainClass, envVars, errorRedirect, outputRedirect,
                                redirectErrorStream, jvmArgs, mainArgs);
        }
    }

    public static Builder builder(Class<?> mainClass) { return new Builder(mainClass); }

    public ChildJVM(Class<?> mainClass, Map<String, String> envVars,
                    ProcessBuilder.Redirect errorRedirect,
                    ProcessBuilder.Redirect outputRedirect,
                    boolean redirectErrorStream,
                    List<String> jvmArgs,
                    List<String> mainArgs) throws IOException {
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

    @Override public void close() throws InterruptedException {
        close(0, 1000, 1000);
    }

    public void close(int waitMs, int destroyWaitMs,
                      int forceWaitMs) throws InterruptedException {
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
