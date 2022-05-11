package com.github.alexishuf.fastersparql.client.model.row;

public interface RowOperationsProvider {
     /**
      * Gets a {@link RowOperations} for instances of {@link RowOperationsProvider#rowClass()}.
      *
      * @return a non-null {@link RowOperations} instance
      * @throws IllegalArgumentException if {@code specializedClass} is not equals to
      *                                   {@link RowOperationsProvider#rowClass()} nor a
      *                                   subclass/subinterface.
      */
     RowOperations get(Class<?> specializedClass);

     /**
      * Which classes the {@link RowOperations} created by {@link RowOperationsProvider#get(Class)}
      * supports.
      *
      * @return a non-null class different from {@code Object.class}
      */
     Class<?> rowClass();
}
