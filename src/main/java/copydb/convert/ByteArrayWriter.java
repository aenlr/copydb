package copydb.convert;

import liquibase.structure.core.Column;

import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class ByteArrayWriter implements ColumnWriter<byte[]> {

    @Override
    public byte[] convert(Column column, Object val) throws SQLException {
        if (val instanceof byte[] b) {
            return b;
        } else if (val instanceof Blob blob) {
            return Converters.extractBytes(blob);
        } else if (val instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        } else if (val instanceof Clob clob) {
            return Converters.extractBytes(clob);
        }

        throw new IllegalArgumentException("Cannot convert " + val + " to " + column);
    }

    @Override
    public byte[] write(Column target, Object val, PreparedStatement stmt, int param) throws SQLException {
        byte[] tgt = convert(target, val);
        stmt.setBytes(param, tgt);
        return tgt;
    }

    static final ByteArrayWriter INSTANCE = new ByteArrayWriter();
}
