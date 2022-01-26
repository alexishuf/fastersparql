package com.github.alexishuf.fastersparql.operators.row;

public interface RowOperationsProvider {
     /**
      * Gets a {@link RowOperations} for instances of {@link RowOperationsProvider#rowClass()}.
      *
      * @return a non-null {@link RowOperations} instance
      */
     RowOperations get();

     /**
      * Which classes the {@link RowOperations} created by {@link RowOperationsProvider#get()}
      * supports.
      *
      * @return a non-null class different from {@code Object.class}
      */
     Class<?> rowClass();
}
