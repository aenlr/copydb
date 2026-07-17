package copydb.convert;

import liquibase.structure.core.Column;

import javax.sql.rowset.serial.SerialBlob;
import java.sql.Blob;
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
        }
        throw new IllegalArgumentException("Cannot convert " + val + " to " + target);
    }

}
