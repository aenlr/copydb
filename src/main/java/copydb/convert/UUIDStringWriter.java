package copydb.convert;

import liquibase.structure.core.Column;

import java.util.UUID;

class UUIDStringWriter extends StringWriter {

    @Override
    public String convert(Column column, Object val) {
        if (val instanceof String s) {
            return s;
        } else if (val instanceof byte[] b && b.length == 16) {
            return Converters.uuidFromBytes(b).toString();
        } else if (val instanceof UUID uuid) {
            return uuid.toString();
        }

        throw new IllegalArgumentException("Cannot convert " + val + " to " + column);
    }

    static final UUIDStringWriter INSTANCE = new UUIDStringWriter();
}
