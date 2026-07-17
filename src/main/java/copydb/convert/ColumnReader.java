package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface ColumnReader<T> {

    T convert(Column column, Object val) throws SQLException;

    default T get(Column column, ResultSet rs) throws SQLException {
        var val = rs.getObject(column.getName());
        return val == null ? null : convert(column, val);
    }

}
