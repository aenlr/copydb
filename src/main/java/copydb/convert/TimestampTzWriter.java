package copydb.convert;

import liquibase.structure.core.Column;

import java.time.OffsetDateTime;

public class TimestampTzWriter implements ColumnWriter<OffsetDateTime> {

    @Override
    public OffsetDateTime convert(Column column, Object val) {
        return TimestampTzReader.toOffsetDateTime(val);
    }

    public static final TimestampTzWriter INSTANCE = new TimestampTzWriter();
}
