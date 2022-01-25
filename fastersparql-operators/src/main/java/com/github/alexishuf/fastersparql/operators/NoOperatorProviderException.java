package com.github.alexishuf.fastersparql.operators;

import lombok.Getter;

import java.util.NoSuchElementException;

import static java.lang.String.format;

public class NoOperatorProviderException extends NoSuchElementException {
    @Getter private final Class<? extends Operator> operatorClass;
    @Getter private final long flags;

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
}
