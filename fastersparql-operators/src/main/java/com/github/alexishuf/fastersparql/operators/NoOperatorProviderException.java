package com.github.alexishuf.fastersparql.operators;

import java.util.NoSuchElementException;

import static java.lang.String.format;

@SuppressWarnings("unused")
public class NoOperatorProviderException extends NoSuchElementException {
    private final Class<? extends Operator> operatorClass;
    private final long flags;

    public NoOperatorProviderException(Class<? extends Operator> operatorClass, long flags,
                                       String message) {
        super(message);
        this.operatorClass = operatorClass;
        this.flags = flags;
    }

    public static NoOperatorProviderException
    noProviders(Class<? extends Operator> operator, long flags) {
        return new NoOperatorProviderException(operator, flags,
                "No "+operator.getSimpleName()+"Providers found");
    }
    public static NoOperatorProviderException
    rejectingProviders(Class<? extends Operator> operator, long flags) {
        return new NoOperatorProviderException(operator, flags,
                format("All "+operator.getSimpleName()+"Providers rejected flags 0x%016x", flags));
    }

    public Class<? extends Operator> operatorClass() { return operatorClass; }
    public long flags() { return flags; }
}
