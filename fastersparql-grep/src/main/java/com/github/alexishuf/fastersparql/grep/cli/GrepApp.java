package com.github.alexishuf.fastersparql.grep.cli;

import com.github.alexishuf.fastersparql.grep.AppException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IExecutionExceptionHandler;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static java.nio.charset.StandardCharsets.UTF_8;

@Command(name = "grep", description = "Filter journal files and netty channel dumps",
         showDefaultValues = true,
         subcommands = {GrepJournal.class, GrepNetty.class})
public class GrepApp {
    public static void main(String[] args) {
        //noinspection InstantiationOfUtilityClass
        int status = new CommandLine(new GrepApp())
                .setExecutionExceptionHandler(new LogExceptionHandler())
                .execute(args);
        System.exit(status);
    }

    private static final class LogExceptionHandler implements IExecutionExceptionHandler {
        @Override
        public int handleExecutionException(Exception ex, CommandLine cl,
                                            CommandLine.ParseResult parseResult) {
            String msg;
            if (ex instanceof AppException) {
                msg = ex.getMessage();
            } else {
                var bos = new ByteArrayOutputStream();
                try (var w = new PrintStream(bos)) {
                    ex.printStackTrace(w);
                }
                msg = bos.toString(UTF_8);
            }
            cl.getErr().println(cl.getColorScheme().errorText(msg));
            var mapper = cl.getExitCodeExceptionMapper();
            return mapper != null ? mapper.getExitCode(ex)
                                  : cl.getCommandSpec().exitCodeOnExecutionException();
        }
    }

}
