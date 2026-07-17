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
import java.util.Optional;

import static copydb.convert.TimestampReader.ISO8601_TIMESTAMP_PATTERN;
import static copydb.convert.TimestampReader.JDBC_TIMESTAMP_PATTERN;
import static copydb.convert.TimestampReader.WELL_KNOWN_TIMESTAMP_TYPES;

public class TimestampTzReader implements ColumnReader<OffsetDateTime> {

    public static Optional<OffsetDateTime> toOffsetDateTime(Object val) {
        if (val instanceof OffsetDateTime dt) {
            return Optional.of(dt);
        }

        if (val instanceof java.util.Date date) {
            int ofs = date.getTimezoneOffset();
            return Optional.of(date.toInstant().atOffset(ZoneOffset.ofHoursMinutes(ofs / 60, ofs % 60)));
        } else if (val instanceof Instant ts) {
            return Optional.of(ts.atZone(ZoneOffset.systemDefault()).toOffsetDateTime());
        } else if (val instanceof LocalDateTime dt) {
            return Optional.of(dt.atZone(ZoneOffset.systemDefault()).toOffsetDateTime());
        } else if (val instanceof ZonedDateTime dt) {
            return Optional.of(dt.toOffsetDateTime());
        } else if (WELL_KNOWN_TIMESTAMP_TYPES.contains(val.getClass().getName())) {
            var ts = Timestamp.valueOf(val.toString());
            int ofs = ts.getTimezoneOffset();
            return Optional.of(ts.toInstant().atOffset(ZoneOffset.ofHoursMinutes(ofs / 60, ofs % 60)));
        } else if (val instanceof String s) {
            if (JDBC_TIMESTAMP_PATTERN.matcher(s).find()) {
                s = s.replaceFirst(" ", "T");
            } else if (!ISO8601_TIMESTAMP_PATTERN.matcher(s).find()) {
                return Optional.empty();
            }

            try {
                return Optional.of(OffsetDateTime.parse(s));
            } catch (DateTimeParseException _ignored) {
                // IGNORED
            }
        }

        return Optional.empty();
    }

    @Override
    public OffsetDateTime convert(Column column, Object val) throws SQLException {
        return toOffsetDateTime(val)
            .orElseThrow(() -> new IllegalArgumentException("Cannot convert " + val + " to timestamp"));
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

    static final ColumnReader<OffsetDateTime> INSTANCE = new TimestampTzReader();
}
