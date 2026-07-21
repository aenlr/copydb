package copydb.convert;

import liquibase.structure.core.Column;

import javax.sql.rowset.serial.SerialBlob;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Writer for cases where an actual Blob object is required
 * and a byte array cannot be used instead.
 * This includes PostgresSQL OID/LO/BLOB columns, whereas Oracle
 * and H2 accept a byte array.
 */
public class BlobWriter implements ColumnWriter<Blob> {

    @Override
    public Blob convert(Column target, Object val) throws SQLException {
        if (val instanceof byte[] b) {
            return new SerialBlob(b);
        } else if (val instanceof Blob blob)  {
            return new SerialBlob(blob);
        } else if (val instanceof Clob clob) {
            return new SerialBlob(Converters.extractBytes(clob));
        } else if (val instanceof String s) {
            return new SerialBlob(s.getBytes(StandardCharsets.UTF_8));
        }

        throw new IllegalArgumentException("Cannot convert " + val + " to " + target);
    }

    @Override
    public Blob write(Column target, Object val, PreparedStatement stmt, int param) throws SQLException {
        var blob = convert(target, val);
        stmt.setBlob(param, blob);
        return blob;
    }

    static final BlobWriter INSTANCE = new BlobWriter();
}
