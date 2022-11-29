package com.github.alexishuf.fastersparql.client.model.row.types;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;

import java.util.ArrayList;
import java.util.List;

public class ListRow<I> extends RowType<List<I>, I> {
    public static final ListRow<String> STRING = new ListRow<>(String.class);

    public ListRow(Class<I> itemClass) {
        //noinspection unchecked
        super((Class<List<I>>)(Class<?>) List.class, itemClass);
    }

    @Override public I set(List<I> row, int idx, I object) {
        return row == null ? null : row.set(idx, object);
    }

    @Override public I get(List<I> row, int idx) {
        return row == null ? null : row.get(idx);
    }

    @Override public List<I> createEmpty(Vars vars) {
        int size = vars.size();
        List<I> row = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
            row.add(null);
        return row;
    }
}
