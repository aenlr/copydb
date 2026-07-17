package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.Blob;
import java.sql.SQLException;

class ByteArrayWriter implements ColumnWriter<byte[]> {
    @Override
    public byte[] convert(Column column, Object val) throws SQLException {
        if (val instanceof byte[] b) {
            return b;
        } else if (val instanceof Blob blob) {
            return Converters.extractBytes(blob);
        }

        throw new IllegalArgumentException("Cannot convert " + val + " to " + column);
    }

    static final ByteArrayWriter INSTANCE = new ByteArrayWriter();
}
