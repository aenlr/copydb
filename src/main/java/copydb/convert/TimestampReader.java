package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.regex.Pattern;

public class TimestampReader implements ColumnReader<Timestamp> {

    static final Set<String> WELL_KNOWN_TIMESTAMP_TYPES = Set.of(
        "oracle.sql.TIMESTAMP"
    );

    static final Pattern JDBC_TIMESTAMP_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
    static final Pattern ISO8601_TIMESTAMP_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}");

    @Override
    public Timestamp convert(Column column, Object val) throws SQLException {
        if (val instanceof Timestamp ts) {
            return ts;
        } else if (val instanceof OffsetDateTime dt) {
            return Timestamp.from(dt.toInstant());
        } else if (val instanceof ZonedDateTime dt) {
            return Timestamp.from(dt.toInstant());
        } else if (val instanceof Instant ts) {
            return Timestamp.from(ts);
        } else if (TimestampReader.WELL_KNOWN_TIMESTAMP_TYPES.contains(val.getClass().getName())) {
            return Timestamp.valueOf(val.toString());
        } else if (val instanceof String s) {
            if (ISO8601_TIMESTAMP_PATTERN.matcher(s).find()) {
                s = s.substring(0, 10) + ' ' + s.substring(11);
            }
            return Timestamp.valueOf(s);
        }
        throw new IllegalArgumentException("Cannot convert " + val + " to timestamp");
    }

    @Override
    public Timestamp get(Column column, ResultSet rs) throws SQLException {
        return rs.getTimestamp(column.getName());
    }

    static final ColumnReader<Timestamp> INSTANCE = new TimestampReader();

}
