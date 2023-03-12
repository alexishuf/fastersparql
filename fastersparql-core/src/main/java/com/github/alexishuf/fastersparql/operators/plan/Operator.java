package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.model.BindType;

public enum Operator {
    JOIN,
    UNION,
    LEFT_JOIN,
    EXISTS,
    NOT_EXISTS,
    MINUS,
    MODIFIER,
    QUERY,
    TRIPLE,
    VALUES,
    EMPTY;

    public BindType bindType() {
        return switch (this) {
            case JOIN -> BindType.JOIN;
            case LEFT_JOIN -> BindType.LEFT_JOIN;
            case EXISTS ->  BindType.EXISTS;
            case NOT_EXISTS ->  BindType.NOT_EXISTS;
            case MINUS -> BindType.MINUS;
            default -> null;
        };
    }
}
