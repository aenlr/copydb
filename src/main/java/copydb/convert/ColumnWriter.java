package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface ColumnWriter<T> {

    T convert(Column target, Object val) throws SQLException;

    default T write(Column target, Object val, PreparedStatement stmt, int param) throws SQLException {
        T tgt = convert(target, val);
        stmt.setObject(param, tgt);
        return tgt;
    }

}
