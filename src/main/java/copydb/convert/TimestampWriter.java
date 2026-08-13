package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

public class TimestampWriter implements ColumnWriter<Timestamp> {

    public static Timestamp toTimestamp(Object val) {
        return switch (val) {
            case null -> null;
            case Timestamp ts -> ts.getClass() != Timestamp.class
                ? Timestamp.valueOf(ts.toString())
                : ts;
            case Instant ts -> Timestamp.from(ts);
            case LocalDateTime dt -> Timestamp.valueOf(dt);
            case OffsetDateTime dt -> Timestamp.from(dt.toInstant());
            case ZonedDateTime dt -> Timestamp.from(dt.toInstant());
            case java.util.Date date -> new Timestamp(date.getTime());
            default -> Timestamp.valueOf(val.toString());
        };
    }

    @Override
    public Timestamp convert(Column column, Object val) {
        return toTimestamp(val);
    }

    @Override
    public Timestamp write(Column target, Object val, PreparedStatement stmt, int param) throws SQLException {
        var ts = convert(target, val);
        stmt.setTimestamp(param, ts);
        return ts;
    }

    public static final TimestampWriter INSTANCE = new TimestampWriter();
}
