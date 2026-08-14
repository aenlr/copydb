package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

import static copydb.convert.TimestampReader.ISO8601_TIMESTAMP_PATTERN;
import static copydb.convert.TimestampReader.JDBC_TIMESTAMP_PATTERN;
import static copydb.convert.TimestampReader.WELL_KNOWN_TIMESTAMP_TYPES;

public class TimestampTzReader implements ColumnReader<OffsetDateTime> {

    public static OffsetDateTime toOffsetDateTime(Object val) {
        return switch (val) {
            case OffsetDateTime dt -> dt;
            case java.util.Date date -> convert(date);
            case Instant ts -> convert(ts);
            case LocalDateTime dt -> dt.atZone(ZoneOffset.systemDefault()).toOffsetDateTime();
            case ZonedDateTime dt -> dt.toOffsetDateTime();
            case String s -> convert(s);
            default -> convertWellKnown(val);
        };
    }

    private static OffsetDateTime convertWellKnown(Object val) {
        if (WELL_KNOWN_TIMESTAMP_TYPES.contains(val.getClass().getName())) {
            return convert(Timestamp.valueOf(val.toString()).toInstant());
        }
        throw new IllegalArgumentException("Cannot convert " + val + " to timestamp");
    }

    private static OffsetDateTime convert(java.util.Date date) {
        return convert(date.toInstant());
    }

    private static OffsetDateTime convert(Instant instant) {
        return instant.atZone(ZoneOffset.systemDefault()).toOffsetDateTime();
    }

    private static OffsetDateTime convert(String s) {
        if (JDBC_TIMESTAMP_PATTERN.matcher(s).find()) {
            s = s.replaceFirst(" ", "T");
        } else if (!ISO8601_TIMESTAMP_PATTERN.matcher(s).find()) {
            return null;
        }

        try {
            return OffsetDateTime.parse(s);
        } catch (DateTimeParseException _ignored) {
            // IGNORED
        }
        return null;
    }

    @Override
    public OffsetDateTime convert(Column column, Object val) throws SQLException {
        return toOffsetDateTime(val);
    }

    @Override
    public OffsetDateTime get(Column column, ResultSet rs) throws SQLException {
        try {
            return rs.getObject(column.getName(), OffsetDateTime.class);
        } catch (SQLException _ignored) {
            // IGNORED
        }

        return convert(column, rs.getTimestamp(column.getName()));
    }

    public static final ColumnReader<OffsetDateTime> INSTANCE = new TimestampTzReader();
}
