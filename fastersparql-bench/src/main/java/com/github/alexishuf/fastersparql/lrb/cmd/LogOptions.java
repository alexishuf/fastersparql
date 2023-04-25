package com.github.alexishuf.fastersparql.lrb.cmd;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static java.util.Arrays.asList;

@Command
@SuppressWarnings({"unused", "FieldCanBeLocal"})
public class LogOptions {
    private @Nullable LevelName level;
    private @Nullable LevelName rootLevel;

    public enum LevelName {
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
    @Option(names = {"-v", "--verbosity"},
            description = "Level of log messages for com.github.alexishuf.fastersparql")
    public void setLevel(LevelName level) {
        this.level = level;
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        Level ll = level.asLogbackLevel();
        for (String suffix : asList("", ".experiments"))
            ctx.getLogger("com.github.alexishuf.fastersparql" + suffix).setLevel(ll);
    }

    @Option(names = "--root-verbosity",
            description = "Level of log messages for the root logger (affects everything outside" +
                    "com.github.alexishuf.fastersparql)")
    public void setRootLevel(LevelName level) {
        this.rootLevel = level;
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        ctx.getLogger(Logger.ROOT_LOGGER_NAME).setLevel(level.asLogbackLevel());
    }

}
