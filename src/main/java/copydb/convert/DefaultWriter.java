package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Timestamp;

public class DefaultWriter implements ColumnWriter<Object> {

    @Override
    public Object convert(Column column, Object val) throws SQLException {
        if (val instanceof Clob clob) {
            return Converters.extractString(clob);
        }

        if (val instanceof Blob blob) {
            return Converters.extractBytes(blob);
        }

        if (val instanceof Timestamp ts) {
            if (ts.getClass() != Timestamp.class) {
                return Timestamp.valueOf(ts.toString());
            }
        } else if (val instanceof java.sql.Date d) {
            if (d.getClass() != java.sql.Date.class) {
                return java.sql.Date.valueOf(d.toString());
            }
        } else if (val instanceof java.util.Date d) {
            if (d.getClass() != java.util.Date.class) {
                return new java.util.Date(d.getTime());
            }
        }

        return val;
    }

    static final ColumnWriter<Object> INSTANCE = new DefaultWriter();
}
