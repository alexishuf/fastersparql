package com.github.alexishuf.fastersparql.util;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

public interface StreamNode {
    Stream<? extends StreamNode> upstream();

    default String nodeLabel()                    { return toString(); }
    default String     toDOT()                    { return StreamNodeDOTGenerator.toDOT(this); }
    @SuppressWarnings("unused") default void viewDOT() throws IOException { viewDOT("xdg-open"); }

    default void viewDOT(String openCommand) throws IOException {
        Path in = Files.createTempFile("fastersparql", ".dot");
        var svg = in.resolveSibling(in.getFileName().toString().replace(".dot", ".svg"));
        try (var w = new FileWriter(in.toFile(), UTF_8)) {
            w.append(toDOT());
        }
        var dot = new ProcessBuilder()
                .command("dot", "-Tsvg", "-o", svg.toString(), in.toString())
                .inheritIO().start();
        try {
            if (dot.waitFor(10, TimeUnit.SECONDS)) {
                if (dot.exitValue() == 0) {
                    svg.toFile().deleteOnExit();
                    new ProcessBuilder().command(openCommand, svg.toString()).start();
                }
            } else {
                dot.destroy();
                if (!dot.waitFor(1, TimeUnit.SECONDS)) dot.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            Files.delete(in);
        }
    }
}
