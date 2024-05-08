package com.github.alexishuf.fastersparql.util;

import com.github.alexishuf.fastersparql.util.concurrent.JournalNamed;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("unused") // this interface is used only for debugging
public interface StreamNode extends JournalNamed {
    Stream<? extends StreamNode> upstreamNodes();

    @Override default String journalName() { return label(StreamNodeDOT.Label.MINIMAL); }

    default String   label(StreamNodeDOT.Label type) {
        return StreamNodeDOT.minimalLabel(new StringBuilder(), this).toString();
    }
    default String   toDOT(StreamNodeDOT.Label type)                    { return StreamNodeDOT.toDOT(this, type); }
    default void   viewDOT(StreamNodeDOT.Label type) throws IOException { viewDOT("xdg-open", type); }

    default void renderDOT(File svgDst, StreamNodeDOT.Label type) throws IOException {
        Path in = Files.createTempFile("fastersparql", ".dot");
        try {
            try (var w = new FileWriter(in.toFile(), UTF_8)) {
                w.append(toDOT(type));
            }
            var dot = new ProcessBuilder()
                    .command("dot", "-Tsvg", "-o", svgDst.toString(), in.toString())
                    .inheritIO().start();
            if (!dot.waitFor(10, TimeUnit.SECONDS) || dot.exitValue() != 0) {
                dot.destroy();
                if (!dot.waitFor(1, TimeUnit.SECONDS))
                    dot.destroyForcibly();
                throw new IOException("dot -Tsvg -o " + svgDst + " " + in + "failed");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            Files.delete(in);
        }
    }

    default void viewDOT(String openCommand, StreamNodeDOT.Label type) throws IOException {
        var svg = Files.createTempFile("fastersparql", ".svg").toFile();
        svg.deleteOnExit();
        renderDOT(svg, type);
        new ProcessBuilder().command(openCommand, svg.toString()).start();
    }
}
