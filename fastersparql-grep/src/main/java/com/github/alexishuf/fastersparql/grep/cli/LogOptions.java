package com.github.alexishuf.fastersparql.grep.cli;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public class LogOptions {
    public enum Verbosity {
        ERROR,
        WARN,
        INFO,
        DEBUG,
        TRACE;

        public Level asLogbackLevel() {
            return switch (this) {
                case ERROR -> Level.ERROR;
                case WARN -> Level.WARN;
                case INFO -> Level.INFO;
                case DEBUG -> Level.DEBUG;
                case TRACE -> Level.TRACE;
            };
        }
    }

    @Option(names = "--verbosity", description = "Set logger verbosity level for " +
            "com.alexishuf.fastersparql package")
    public void setVerbosity(Verbosity level) {
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        Level ll = level.asLogbackLevel();
        ctx.getLogger("com.github.alexishuf.fastersparql").setLevel(ll);
    }
}
