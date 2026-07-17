package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

public class TimestampWriter implements ColumnWriter<Timestamp> {


    public static Timestamp toTimestamp(Object val) {
        // TODO: transform to pattern matching in Java 21
        if (val instanceof Timestamp ts) {
            if (ts.getClass() != Timestamp.class) {
                return Timestamp.valueOf(ts.toString());
            }

            return ts;
        } else if (val instanceof Instant ts) {
            return Timestamp.from(ts);
        } else if (val instanceof LocalDateTime dt) {
            return Timestamp.valueOf(dt);
        } else if (val instanceof OffsetDateTime dt) {
            return Timestamp.from(dt.toInstant());
        } else if (val instanceof ZonedDateTime dt) {
            return Timestamp.from(dt.toInstant());
        } else if (val instanceof java.util.Date date) {
            return new Timestamp(date.getTime());
        } else {
            return Timestamp.valueOf(val.toString());
        }
    }

    @Override
    public Timestamp convert(Column column, Object val) {
        return toTimestamp(val);
    }

    static final TimestampWriter INSTANCE = new TimestampWriter();
}
