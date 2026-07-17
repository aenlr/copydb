package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.HexFormat;
import java.util.UUID;

public class UUIDBytesWriter implements ColumnWriter<byte[]> {

    @Override
    public byte[] convert(Column column, Object val) throws SQLException {
        if (val instanceof String s) {
            if (s.length() == 36) {
                try {
                    UUID uuid = UUID.fromString(s);
                    return Converters.uuidToBytes(uuid);
                } catch (IllegalArgumentException _ignored) {
                    // IGNORED
                }
            } else if (s.length() == 32 && s.matches("[0-9a-fA-F]*")) {
                return HexFormat.of().parseHex(s);
            }
        } else if (val instanceof UUID uuid) {
            return Converters.uuidToBytes(uuid);
        } else if (val instanceof byte[] b) {
            return b;
        } else if (val instanceof Blob blob) {
            return Converters.extractBytes(blob);
        }

        throw new IllegalArgumentException("Cannot convert " + val + " to " + column);
    }

    static final UUIDBytesWriter INSTANCE = new UUIDBytesWriter();
}
