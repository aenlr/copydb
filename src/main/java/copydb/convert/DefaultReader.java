package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.SQLException;

public class DefaultReader implements ColumnReader<Object> {
    @Override
    public Object convert(Column column, Object val) throws SQLException {
        return val;
    }

    public static final ColumnReader<Object> INSTANCE = new DefaultReader();
}
