package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Database column writer.
 */
@FunctionalInterface
public interface ColumnWriter<T> {

    /**
     * Convert a value to target column type.
     * @param target target column
     * @param val value to convert (non-null)
     * @return converted value (non-null)
     * @apiNote This method never has to deal with null values. It will never be
     * passsed a null value and must never return a null value.
     */
    T convert(Column target, Object val) throws SQLException;

    /**
     * Set value for a prepared statement.
     * @param target target column
     * @param val value to set
     * @param stmt JDBC prepared statement
     * @param param parameter index to set
     * @apiNote This method never has to deal with null values. It will never be
     * passsed a null value.
     */
    default T write(Column target, Object val, PreparedStatement stmt, int param) throws SQLException {
        T tgt = convert(target, val);
        stmt.setObject(param, tgt);
        return tgt;
    }

}
