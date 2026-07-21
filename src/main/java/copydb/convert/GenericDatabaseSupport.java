package copydb.convert;

import liquibase.database.Database;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class GenericDatabaseSupport implements DatabaseSupport {

    protected GenericDatabaseSupport() {
    }

    private static final Set<String> STANDARD_NUMERIC_TYPES = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private static final Map<String, ColumnReader<?>> READERS = new HashMap<>();
    private static final Map<String, ColumnWriter<?>> WRITERS = new HashMap<>();

    static {
        STANDARD_NUMERIC_TYPES.addAll(List.of(
            "NUMERIC",
            "DECIMAL",
            "SMALLINT",
            "INT",
            "INTEGER",
            "BIGINT",
            "FLOAT",
            "REAL",
            "DOUBLE PRECISION",
            "DECFLOAT"
        ));

        for (var k : STANDARD_NUMERIC_TYPES) {
            READERS.put(k, NumberReader.INSTANCE);
            WRITERS.put(k, NumberWriter.INSTANCE);
        }

        WRITERS.put("BLOB", BlobWriter.INSTANCE);
        WRITERS.put("BINARY", ByteArrayWriter.INSTANCE);
        WRITERS.put("VARBINARY", ByteArrayWriter.INSTANCE);
        WRITERS.put("BINARY VARYING", ByteArrayWriter.INSTANCE);
    }

    /**
     * Test if type is a standard string/text type including CLOB but excluding NCHAR types.
     */
    static boolean isCharacterType(String typeName) {
        return isJavaStringColumn(typeName)
            || "CLOB".equalsIgnoreCase(typeName)
            || "CHARACTER LARGE OBJECT".equalsIgnoreCase(typeName);
    }

    /**
     * Test if type is a standard string/text type excluding CLOB and NCHAR types.
     */
    static boolean isJavaStringColumn(String typeName) {
        return "VARCHAR".equalsIgnoreCase(typeName)
            || "VARCHAR2".equalsIgnoreCase(typeName)
            || "CHAR".equalsIgnoreCase(typeName)
            || "CHARACTER".equals(typeName)
            || "CHARACTER VARYING".equals(typeName)
            || "TEXT".equalsIgnoreCase(typeName);
    }

    /**
     * Test if type is a timestamp type with or without time zone.
     */
    static boolean isTimestampType(Column column) {
        DataType type = column.getType();
        String name = type.getTypeName().toUpperCase(Locale.ROOT);
        return "TIMESTAMP".equals(name) || name.startsWith("TIMESTAMP(")
            || name.equals("TIMESTAMPTZ") || name.startsWith("TIMESTAMPTZ(") // PostgreSQL
            || name.startsWith("TIMESTAMP ");
    }

    /**
     * Test if a timestamp type includes time zone.
     */
    static boolean isTimestampTzType(Column column) {
        DataType type = column.getType();
        String name = type.getTypeName().toUpperCase(Locale.ROOT);
        return name.endsWith("WITH TIME ZONE")
            || "TIMESTAMPTZ".equals(name) || name.startsWith("TIMESTAMPTZ(") // PostgreSQL
            ;
    }

    @Override
    public ColumnReader<?> readerFor(Database source, Column sourceColumn) {
        String targetType = sourceColumn.getType().getTypeName();
        var reader = READERS.get(targetType);
        if (reader != null) {
            return reader;
        }

        if (isTimestampType(sourceColumn)) {
            return GenericDatabaseSupport.isTimestampTzType(sourceColumn) ? TimestampTzReader.INSTANCE : TimestampReader.INSTANCE;
        }

        return DefaultReader.INSTANCE;
    }

    @Override
    public ColumnWriter<?> writerFor(Database target, Column targetColumn) {
        DataType type = targetColumn.getType();
        String name = type.getTypeName();
        var writer = WRITERS.get(name);
        if (writer != null) {
            return writer;
        }

        if (isTimestampType(targetColumn)) {
            if (isTimestampTzType(targetColumn)) {
                return TimestampTzWriter.INSTANCE;
            } else {
                return TimestampWriter.INSTANCE;
            }
        }

        // Something to a proper UUID column
        if ("UUID".equalsIgnoreCase(name)) {
            return UUIDWriter.INSTANCE;
        }

        // Something to what might be a UUID in canonical text representation
        if (type.getColumnSize() != null
            && type.getColumnSize() == 36
            && isJavaStringColumn(name)) {
            return UUIDStringWriter.INSTANCE;
        }

        if ("BOOL".equalsIgnoreCase(name) || "BOOLEAN".equalsIgnoreCase(name)) {
            return BooleanWriter.INSTANCE;
        }

        if (isCharacterType(name)) {
            return StringWriter.INSTANCE;
        }

        return DefaultWriter.INSTANCE;
    }


    static final DatabaseSupport INSTANCE = new GenericDatabaseSupport();
}
