package com.github.alexishuf.fastersparql.exceptions;

import org.tomlj.TomlParseError;

import java.util.List;

public class BadSerializationException extends FSInvalidArgument {
    public BadSerializationException(String message) {
        super(message);
    }

    public static class UnknownSelector extends BadSerializationException {
        public UnknownSelector(String name) {
            super("No Loader for Selector "+name+" found in classpath");
        }
    }

    public static class UnknownEstimator extends BadSerializationException {
        public UnknownEstimator(String name) {
            super("No Loader for CardinalityEstimator "+name+" found in classpath");
        }
    }

    public static class SelectorTypeMismatch extends BadSerializationException {
        public final String expected, actual;
        public SelectorTypeMismatch(String expected, String actual) {
            super("Expected state for a "+expected+" selector, but state is for a "+actual+" selector");
            this.expected = expected;
            this.actual = actual;
        }
    }

    public static class UnknownSource extends BadSerializationException {
        public UnknownSource(String name) {
            super("No Loader for Source "+name+" found in classpath");
        }
    }

    public static class TomlError extends BadSerializationException {
        public TomlError(String message) { super(message); }
        public TomlError(List<TomlParseError> errors) { super(buildMessage(errors)); }

        private static String buildMessage(List<TomlParseError> errors) {
            if (errors.isEmpty())
                throw new AssertionError("No errors");
            TomlParseError f = errors.get(0);
            return errors.size()+" errors ,the first of which is at position "+f.position()+": "+f.getMessage();
        }
    }
}
