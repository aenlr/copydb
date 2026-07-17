package copydb.convert;

import liquibase.structure.core.Column;

public class NumberWriter implements ColumnWriter<Number> {

    @Override
    public Number convert(Column column, Object val) {
        if (val instanceof Number n) {
            return n;
        } else if (val instanceof Boolean b) {
            return b ? 1 : 0;
        }

        throw new IllegalArgumentException("Cannot convert " + val + " to " + column);
    }

    static final ColumnWriter<Number> INSTANCE = new NumberWriter();

}
