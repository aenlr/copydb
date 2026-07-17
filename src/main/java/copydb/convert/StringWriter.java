package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.Clob;
import java.sql.SQLException;
import java.util.UUID;

public class StringWriter implements ColumnWriter<String> {

    @Override
    public String convert(Column column, Object val) throws SQLException {
        if (val instanceof String s) {
            return s;
        } else if (val instanceof Clob clob) {
            return Converters.extractString(clob);
        } else if (val instanceof Number n) {
            return n.toString();
        } else if (val instanceof UUID uuid) {
            return uuid.toString();
        }

        throw new IllegalArgumentException("Cannot convert " + val + " to " + column);
    }

    static final ColumnWriter<String> INSTANCE = new StringWriter();
}
