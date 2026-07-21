package copydb.convert;

import liquibase.database.Database;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public final class Converters {

    private static final Map<String, DatabaseSupport> DATABASES = Map.of(
        "oracle", OracleSupport.INSTANCE,
        "postgresql", PostgresSupport.INSTANCE
    );

    private Converters() {
    }

    public static ColumnDescriptor<?, ?> converterFor(Database source, Column sourceColumn,
                                                      Database target, Column targetColumn) {

        var sourceDb = DATABASES.getOrDefault(source.getShortName(), GenericDatabaseSupport.INSTANCE);
        var targetDb = DATABASES.getOrDefault(target.getShortName(), GenericDatabaseSupport.INSTANCE);
        ColumnReader<?> reader = sourceDb.readerFor(source, sourceColumn);
        DataType sourceType = sourceColumn.getType();
        String sourceTypeName = sourceType.getTypeName();

        DataType targetType = targetColumn.getType();
        String targetTypeName = targetType.getTypeName();
        if (sourceTypeName.equalsIgnoreCase(targetTypeName) && targetDb == sourceDb) {
            return new ColumnDescriptor<>(sourceColumn, targetColumn, reader, DefaultWriter.INSTANCE);
        }

        var writer = targetDb.writerFor(target, targetColumn);
        return new ColumnDescriptor<>(sourceColumn, targetColumn, reader, writer);
    }

    public static byte[] extractBytes(Blob blob) throws SQLException {
        long len = blob.length();
        if (len < 8192) {
            return blob.getBytes(1, (int) len);
        }

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (var is = blob.getBinaryStream()) {
            byte[] buf = new byte[4096];
            int n;
            while ((n = is.read(buf)) > 0) {
                os.write(buf, 0, n);
            }
        } catch (IOException e) {
            throw new SQLException("Failed to read BLOB", e);
        }
        return os.toByteArray();
    }

    public static byte[] extractBytes(Clob clob) throws SQLException {
        return extractString(clob).getBytes(StandardCharsets.UTF_8);
    }

    public static String extractString(Clob clob) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (var reader = clob.getCharacterStream()) {
            char[] buf = new char[4096];
            int n;
            while ((n = reader.read(buf)) > 0) {
                sb.append(buf, 0, n);
            }
        } catch (IOException e) {
            throw new SQLException("Failed reading CLOB", e);
        }

        return sb.toString();
    }

    public static UUID uuidFromBytes(byte[] bytes) {
        long msb = ((long) bytes[0] & 0xFF) << 56;
        msb |= ((long) bytes[1] & 0xFF) << 48;
        msb |= ((long) bytes[2] & 0xFF) << 40;
        msb |= ((long) bytes[3] & 0xFF) << 32;
        msb |= ((long) bytes[4] & 0xFF) << 24;
        msb |= ((long) bytes[5] & 0xFF) << 16;
        msb |= ((long) bytes[6] & 0xFF) << 8;
        msb |= bytes[7] & 0xFF;

        long lsb = ((long) bytes[8] & 0xFF) << 56;
        lsb |= ((long) bytes[9] & 0xFF) << 48;
        lsb |= ((long) bytes[10] & 0xFF) << 40;
        lsb |= ((long) bytes[11] & 0xFF) << 32;
        lsb |= ((long) bytes[12] & 0xFF) << 24;
        lsb |= ((long) bytes[13] & 0xFF) << 16;
        lsb |= ((long) bytes[14] & 0xFF) << 8;
        lsb |= bytes[15] & 0xFF;
        return new UUID(msb, lsb);
    }

    public static byte[] uuidToBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

}
