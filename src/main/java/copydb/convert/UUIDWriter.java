package copydb.convert;

import liquibase.structure.core.Column;

import java.util.UUID;

public class UUIDWriter implements ColumnWriter<UUID> {

    public static UUID toUUID(Object val) {
        if (val instanceof String s) {
            return UUID.fromString(s);
        } else if (val instanceof byte[] b && b.length == 16) {
            return Converters.uuidFromBytes(b);
        } else if (val instanceof UUID uuid) {
            return uuid;
        }

        throw new IllegalArgumentException("Cannot convert " + val.getClass() + " to UUID");
    }

    @Override
    public UUID convert(Column column, Object val) {
        return toUUID(val);
    }

    static final UUIDWriter INSTANCE = new UUIDWriter();
}
