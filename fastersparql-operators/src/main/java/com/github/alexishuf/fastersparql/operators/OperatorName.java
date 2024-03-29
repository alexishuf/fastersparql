package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.operators.providers.*;

public enum OperatorName {
    JOIN,
    UNION,
    MERGE,
    LEFT_JOIN,
    SLICE,
    DISTINCT,
    PROJECT,
    FILTER,
    FILTER_EXISTS,
    MINUS;

    public Class<? extends Operator> asClass() {
        switch (this) {
            case JOIN:
                return Join.class;
            case UNION:
                return Union.class;
            case LEFT_JOIN:
                return LeftJoin.class;
            case SLICE:
                return Slice.class;
            case DISTINCT:
                return Distinct.class;
            case PROJECT:
                return Project.class;
            case FILTER:
                return Filter.class;
            case FILTER_EXISTS:
                return FilterExists.class;
            case MINUS:
                return Minus.class;
            default:
                throw new UnsupportedOperationException("No Class<? extends Operator> for"+this);
        }
    }

    public Class<? extends OperatorProvider> providerClass() {
        switch (this) {
            case JOIN:
                return JoinProvider.class;
            case UNION:
                return UnionProvider.class;
            case MERGE:
                return MergeProvider.class;
            case LEFT_JOIN:
                return LeftJoinProvider.class;
            case SLICE:
                return SliceProvider.class;
            case DISTINCT:
                return DistinctProvider.class;
            case PROJECT:
                return ProjectProvider.class;
            case FILTER:
                return FilterProvider.class;
            case FILTER_EXISTS:
                return FilterExistsProvider.class;
            case MINUS:
                return MinusProvider.class;
            default:
                throw new UnsupportedOperationException("No Class<? extends OperatorProvider> for "+this);
        }
    }

    public static OperatorName valueOf(Class<? extends Operator> cls) {
        if      (cls.equals(Join.class))         return JOIN;
        else if (cls.equals(LeftJoin.class))     return LEFT_JOIN;
        else if (cls.equals(Union.class))        return UNION;
        else if (cls.equals(Merge.class))        return MERGE;
        else if (cls.equals(Slice.class))        return SLICE;
        else if (cls.equals(Distinct.class))     return DISTINCT;
        else if (cls.equals(Project.class))      return PROJECT;
        else if (cls.equals(Filter.class))       return FILTER;
        else if (cls.equals(FilterExists.class)) return FILTER_EXISTS;
        else if (cls.equals(Minus.class))        return MINUS;
        else
            throw new IllegalArgumentException(cls+" is not a known Operator");
    }

    public static OperatorName valueOfProvider(Class<? extends OperatorProvider> cls) {
        if      (cls.equals(JoinProvider.class))         return JOIN;
        else if (cls.equals(LeftJoinProvider.class))     return LEFT_JOIN;
        else if (cls.equals(UnionProvider.class))        return UNION;
        else if (cls.equals(MergeProvider.class))        return MERGE;
        else if (cls.equals(SliceProvider.class))        return SLICE;
        else if (cls.equals(DistinctProvider.class))     return DISTINCT;
        else if (cls.equals(ProjectProvider.class))      return PROJECT;
        else if (cls.equals(FilterProvider.class))       return FILTER;
        else if (cls.equals(FilterExistsProvider.class)) return FILTER_EXISTS;
        else if (cls.equals(MinusProvider.class))        return MINUS;
        else
            throw new IllegalArgumentException(cls+" is not a known Operator");
    }
}
