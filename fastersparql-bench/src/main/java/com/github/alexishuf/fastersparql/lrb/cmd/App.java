package com.github.alexishuf.fastersparql.lrb.cmd;

import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;

@Command(name = "experiment",
        description = "Run a benchmark command",
        subcommands = {Measure.class})
public class App {
    public static void main(String[] args) throws IOException {
        System.exit(run(args));
    }

    public static int run(String[] args) {
        //noinspection InstantiationOfUtilityClass
        return new CommandLine(new App()).execute(args);
    }
}
