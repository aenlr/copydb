package copydb.convert;

import liquibase.database.Database;
import liquibase.structure.core.Column;
import org.postgresql.util.PGobject;

import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Map;

import static java.util.Map.entry;

public final class PostgresSupport extends GenericDatabaseSupport {

    private PostgresSupport() {
    }

    private static final Map<String, ColumnReader<?>> READERS = Map.ofEntries(
        entry("json", JsonReader.INSTANCE),
        entry("jsonb", JsonReader.INSTANCE),
        entry("smallserial", NumberReader.INSTANCE),
        entry("serial", NumberReader.INSTANCE),
        entry("bigserial", NumberReader.INSTANCE),
        entry("int2", NumberReader.INSTANCE),
        entry("int4", NumberReader.INSTANCE),
        entry("int8", NumberReader.INSTANCE)
    );

    private static final Map<String, ColumnWriter<?>> WRITERS = Map.ofEntries(
        entry("json", JsonWriter.INSTANCE),
        entry("jsonb", JsonWriter.INSTANCE),
        entry("smallserial", NumberWriter.INSTANCE),
        entry("serial", NumberWriter.INSTANCE),
        entry("bigserial", NumberWriter.INSTANCE),
        entry("int2", NumberWriter.INSTANCE),
        entry("int4", NumberWriter.INSTANCE),
        entry("int8", NumberWriter.INSTANCE)
    );

    @Override
    public ColumnReader<?> readerFor(Database source, Column sourceColumn) {
        String type = sourceColumn.getType().getTypeName();
        var reader = READERS.get(type);
        if (reader != null) {
            return reader;
        }

        return super.readerFor(source, sourceColumn);
    }

    @Override
    public ColumnWriter<?> writerFor(Database target, Column targetColumn) {
        String targetType = targetColumn.getType().getTypeName();
        var writer = WRITERS.get(targetType);
        if (writer != null) {
            return writer;
        }

        return super.writerFor(target, targetColumn);
    }

    static class JsonReader implements ColumnReader<String> {

        @Override
        public String convert(Column column, Object val) throws SQLException {
            if (val instanceof PGobject obj) {
                return obj.getValue();
            } else if (val instanceof String s) {
                return s;
            } else if (val instanceof byte[] b) {
                return new String(b, StandardCharsets.UTF_8);
            }

            throw new IllegalArgumentException("Cannot read JSON from " + val + " [" + column + "]");
        }

        static final JsonReader INSTANCE = new JsonReader();

    }

    static class JsonWriter implements ColumnWriter<PGobject> {
        @Override
        public PGobject convert(Column target, Object val) throws SQLException {
            PGobject pgobj = new PGobject();
            pgobj.setType(target.getType().getTypeName());

            if (val instanceof String s) {
                pgobj.setValue(s);
            } else if (val instanceof Clob clob) {
                pgobj.setValue(Converters.extractString(clob));
            } else if (val instanceof byte[] b) {
                pgobj.setValue(new String(b, StandardCharsets.UTF_8));
            } else if (val instanceof Blob blob) {
                pgobj.setValue(new String(Converters.extractBytes(blob), StandardCharsets.UTF_8));
            } else {
                throw new IllegalArgumentException("Cannot convert " + val + " to JSON [" + target + "]");
            }

            return pgobj;
        }

        private static final JsonWriter INSTANCE = new JsonWriter();
    }

    static final DatabaseSupport INSTANCE = new PostgresSupport();
}
