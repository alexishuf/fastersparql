package com.github.alexishuf.fastersparql.util.owned;

public abstract class AbstractSpecialOwner implements SpecialOwner {
    private final String name;

    public AbstractSpecialOwner(String name) {this.name = name;}

    @Override public String journalName() {return name;}
    @Override public String toString() {return journalName();}
}
