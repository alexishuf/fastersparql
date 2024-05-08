package com.github.alexishuf.fastersparql.util.owned;


import com.github.alexishuf.fastersparql.util.concurrent.JournalNamed;

public interface SpecialOwner extends JournalNamed {

    interface Recycled extends SpecialOwner, LeakyOwner { }
    interface Garbage extends Recycled { }
    interface Hangman extends SpecialOwner { }
    interface Constant extends SpecialOwner, LeakyOwner { }

    /** Owner of all recycled objects */
    Recycled RECYCLED = new RecycledOwner();

    /**
     * Owner of all objects where not be pooled during {@link Owned#recycle(Object)} and thus
     * will eventually be garbage-collected
     */
    Garbage GARBAGE = new GarbageOwner();

    /**
     * An owner that {@link Orphan#takeOwnership(Object)} only to immediately call
     * {@link Owned#recycle(Object)}..
     */
    Hangman HANGMAN = new HangmanOwner();

    /** Owner for global constants */
    Constant CONSTANT = new ConstantOwner();
}


class RecycledOwner extends AbstractSpecialOwner implements SpecialOwner.Recycled {
    public RecycledOwner() {super("RECYCLED");}
}
class GarbageOwner extends AbstractSpecialOwner implements SpecialOwner.Garbage {
    public GarbageOwner() {super("GARBAGE");}
}
class ConstantOwner extends AbstractSpecialOwner implements SpecialOwner.Constant {
    public ConstantOwner() {super("CONSTANT");}
}
class HangmanOwner extends AbstractSpecialOwner implements SpecialOwner.Hangman {
    public HangmanOwner() {super("HANGMAN");}
}
