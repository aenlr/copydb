package copydb.convert;

import liquibase.database.Database;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;

import java.util.Map;

import static java.util.Map.entry;

public final class OracleSupport extends GenericDatabaseSupport {

    private OracleSupport() {
    }

    private static final Map<String, ColumnReader<?>> READERS = Map.of(
        "NUMBER", NumberReader.INSTANCE
    );

    private static final Map<String, ColumnWriter<?>> WRITERS = Map.ofEntries(
        entry("NUMBER", NumberWriter.INSTANCE),
        entry("RAW", ByteArrayWriter.INSTANCE)
    );

    @Override
    public ColumnReader<?> readerFor(Database source, Column sourceColumn) {
        String targetType = sourceColumn.getType().getTypeName();
        var reader = READERS.get(targetType);
        if (reader != null) {
            return reader;
        }
        return super.readerFor(source, sourceColumn);
    }

    @Override
    public ColumnWriter<?> writerFor(Database target, Column targetColumn) {
        DataType type = targetColumn.getType();
        String name = type.getTypeName();
        if ("RAW".equals(name) && type.getColumnSize() != null && type.getColumnSize() == 16) {
            return UUIDBytesWriter.INSTANCE;
        }

        var writer = WRITERS.get(name);
        if (writer != null) {
            return writer;
        }

        return super.writerFor(target, targetColumn);
    }

    static final DatabaseSupport INSTANCE = new OracleSupport();

}
