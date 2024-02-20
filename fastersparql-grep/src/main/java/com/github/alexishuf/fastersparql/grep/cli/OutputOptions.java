package com.github.alexishuf.fastersparql.grep.cli;

import org.checkerframework.checker.nullness.qual.Nullable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.*;

@Command
public class OutputOptions {
    @Spec private CommandSpec spec;

    @Option(names = {"--output", "-o"},
            description = "Write result to given file, which will be truncated if exists")
    private @Nullable Path outFile;

    @Override public String toString() {
        return outFile == null ? "-" : outFile.toString();
    }

    public WritableByteChannel open() {
        if (outFile == null || outFile.toString().equals("-"))
            return Channels.newChannel(System.out);
        var cl = spec.commandLine();
        if (Files.isDirectory(outFile))
            throw new ParameterException(cl, outFile+" already exists as a directory");
        Path dir = outFile.getParent();
        if (Files.notExists(dir)) {
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                throw new ParameterException(cl, "Could not mkdir"+dir);
            }
        } else if (!Files.isDirectory(dir)) {
            throw new ParameterException(cl, "Cannot create "+outFile+" as "+dir+" is a file");
        }
        try {
            return Files.newByteChannel(outFile, WRITE, TRUNCATE_EXISTING, CREATE);
        } catch (IOException e) {
            throw new ParameterException(cl, "Could not create/truncate "+outFile+": "+e.getMessage());
        }
    }

}
