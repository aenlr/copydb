package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Database column reader.
 */
@FunctionalInterface
public interface ColumnReader<T> {

    /**
     * Convert value from database.
     * @param column source column
     * @param val database value
     * @apiNote This function never has to deal with null values.
     */
    T convert(Column column, Object val) throws SQLException;

    /**
     * Read a column value from a result set.
     * @param column source column
     * @param rs database result set
     * @return converted value from database (can be null)
     * @apiNote This function has to deal with null values from the database, but
     * must never call  {@link copydb.convert.ColumnReader#convert(Column, Object)}
     * with a null value.
     */
    default T get(Column column, ResultSet rs) throws SQLException {
        var val = rs.getObject(column.getName());
        return val == null ? null : convert(column, val);
    }

}
