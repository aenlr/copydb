package copydb.db;

import copydb.ObjectFilter;
import copydb.convert.BlobWriter;
import copydb.convert.BooleanWriter;
import copydb.convert.ByteArrayWriter;
import copydb.convert.ColumnReader;
import copydb.convert.ColumnWriter;
import copydb.convert.DefaultReader;
import copydb.convert.DefaultWriter;
import copydb.convert.NumberReader;
import copydb.convert.NumberWriter;
import copydb.convert.StringWriter;
import copydb.convert.TimestampReader;
import copydb.convert.TimestampTzReader;
import copydb.convert.TimestampTzWriter;
import copydb.convert.TimestampWriter;
import copydb.convert.UUIDStringWriter;
import copydb.convert.UUIDWriter;
import liquibase.Scope;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.AlterSequenceStatement;
import liquibase.statement.core.CreateSequenceStatement;
import liquibase.statement.core.DropSequenceStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Sequence;
import liquibase.structure.core.Table;

import java.math.BigInteger;
import java.sql.Savepoint;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class GenericDatabaseSupport implements DatabaseSupport {

    private final String shortName;

    protected GenericDatabaseSupport(String shortName) {
        this.shortName = shortName;
    }

    private static final Set<String> STANDARD_NUMERIC_TYPES = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private static final Map<String, ColumnReader<?>> READERS = new HashMap<>();
    private static final Map<String, ColumnWriter<?>> WRITERS = new HashMap<>();

    private static final BigInteger BIGINT_64_MAX = BigInteger.valueOf(Long.MAX_VALUE);

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
    public String getShortName() {
        return shortName;
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

    @Override
    public BigInteger getSequenceMaxValue() {
        return BIGINT_64_MAX;
    }

    /**
     * Copy a sequence to target database.
     *
     * @param source Source sequence
     * @param db target database
     * @param target existing sequence in target database (or null if it does not exist)
     * @throws LiquibaseException
     */
    @Override
    public void copySequence(Sequence source, Database db, Sequence target) throws LiquibaseException {
        List<SqlStatement> stmts;
        if (target != null) {
            stmts = alterSequenceStatements(source, db, target);
        } else {
            stmts = List.of(createSequenceStatement(source, db));
        }
        execute(db, stmts);
    }

    @Override
    public List<Trigger> getTriggers(Database db) throws LiquibaseException {
        throw new UnsupportedOperationException("Cannot enumerate triggers for " + db.getShortName());
    }

    @Override
    public Action disableTriggers(Database db, ObjectFilter filter) throws LiquibaseException {
        if (!filter.isEnabled() || filter.getExclude().contains("*")) {
            return Action.NOP;
        }

        throw new UnsupportedOperationException("Cannot disable triggers for " + db.getShortName());
    }

    @Override
    public Action disableTriggers(Database db, List<Table> tables, ObjectFilter filter) throws LiquibaseException {
        return disableTriggers(db, filter);
    }

    @Override
    public Action disableForeignKeys(Database db, List<ForeignKey> keys) throws LiquibaseException {
        if (keys.isEmpty()) {
            return Action.NOP;
        }

        throw new UnsupportedOperationException("Cannot disable foreign keys for " +  db.getShortName());
    }

    /**
     * Generate statements to alter an existing sequence to match a given state.
     * @param source sequence to match
     * @param db target database
     * @param target existing sequence in target database
     * @return list of statements to alter or recreate sequence (empty if target sequence is identical)
     */
    protected Optional<AlterSequenceStatement> diffSequence(Sequence source, Database db, Sequence target) {
        var alter = new AlterSequenceStatement(db.getDefaultCatalogName(), db.getDefaultSchemaName(), target.getName());
        // TODO: introspection returns sequences with name only on H2 and are therefore always altered
        boolean changed = !Objects.equals(source.getStartValue(), target.getStartValue());

        if (!Objects.equals(source.getCacheSize(), target.getCacheSize())) {
            alter.setCacheSize(source.getCacheSize());
            changed = true;
        }
        if (!Objects.equals(source.getIncrementBy(), target.getIncrementBy())) {
            alter.setIncrementBy(source.getIncrementBy());
            changed = true;
        }

        if (!Objects.equals(source.getMinValue(), target.getMinValue())) {
            alter.setMinValue(source.getMinValue());
            changed = true;
        }

        var sourceMax = source.getMaxValue();
        var targetMax = target.getMaxValue();
        if (sourceMax != null && targetMax != null && !Objects.equals(sourceMax, targetMax)) {
            if (sourceMax.compareTo(targetMax) > 0) {
                var targetMaxLimit = getSequenceMaxValue();
                if (targetMaxLimit != null && sourceMax.compareTo(targetMaxLimit) > 0) {
                    if (!targetMax.equals(targetMaxLimit)) {
                        alter.setMaxValue(targetMaxLimit);
                        changed = true;
                    }
                } else {
                    if (!targetMax.equals(targetMaxLimit)) {
                        alter.setMaxValue(sourceMax);
                        changed = true;
                    }
                }
            } else {
                alter.setMaxValue(sourceMax);
                changed = true;
            }
        }

        return changed ? Optional.of(alter) : Optional.empty();
    }

    @SuppressWarnings("deprecation")
    protected final List<SqlStatement> alterSequenceRestart(Sequence source, Database db, Sequence target, Function<BigInteger, String> formatRestart) {
        var alter = diffSequence(source, db, target).orElse(null);
        if (alter == null) {
            return List.of();
        }

        BigInteger startValue = source.getStartValue();
        if (Objects.equals(startValue, target.getStartValue())) {
            return List.of(alter);
        }

        Sql[] statements = SqlGeneratorFactory.getInstance().generateSql(alter, db);
        if (statements.length != 1) {
            throw new IllegalStateException("Unexpected number of alter sequence statements: " + statements.length);
        }

        String sql = statements[0].toSql();
        Pattern pattern = Pattern.compile("^(ALTER SEQUENCE\\s+[^ ]+)(.*)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalStateException("Unexpected alter sequence statement: " + sql);
        }

        sql = matcher.group(1) + " " + formatRestart.apply(startValue) + " " + matcher.group(2);
        // Not RawParameterizedSqlStatement since it bypasses logging listener
        return List.of(new RawSqlStatement(sql));
    }


    protected List<SqlStatement> alterSequenceStatements(Sequence source, Database db, Sequence target) {
        return diffSequence(source, db, target)
            .stream()
            .flatMap(alter -> {
                SqlStatement drop = new DropSequenceStatement(alter.getCatalogName(), alter.getSchemaName(), alter.getSequenceName());
                SqlStatement create = createSequenceStatement(source, db);
                return Stream.of(drop, create);
            })
            .toList();
    }

    protected CreateSequenceStatement createSequenceStatement(Sequence seq, Database db) {
        var stmt = new CreateSequenceStatement(db.getDefaultCatalogName(), db.getDefaultSchemaName(), seq.getName());
        stmt.setCacheSize(seq.getCacheSize());
        stmt.setIncrementBy(seq.getIncrementBy());
        stmt.setMinValue(seq.getMinValue());
        if (seq.getMaxValue() != null) {
            var targetMax = getSequenceMaxValue();
            if (targetMax != null && seq.getMaxValue().compareTo(targetMax) > 0) {
                stmt.setMaxValue(targetMax);
            } else if (targetMax == null || !seq.getMaxValue().equals(targetMax)) {
                stmt.setMaxValue(seq.getMaxValue());
            }
        }
        stmt.setOrdered(seq.getOrdered());
        stmt.setCycle(seq.getWillCycle());
        stmt.setStartValue(seq.getStartValue());

        return stmt;
    }

    @Override
    public <T> Optional<T> querySingle(Database db,
                                       String comment,
                                       String query,
                                       Class<T> clazz) throws LiquibaseException {
        var conn = (JdbcConnection) db.getConnection();
        var executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", db);
        executor.comment(comment);

        Savepoint savepoint = conn.setSavepoint();
        try {
            @SuppressWarnings("deprecation")
            var sql = new RawSqlStatement(query);
            var result = executor.queryForObject(sql, clazz);
            conn.rollback(savepoint);
            return Optional.ofNullable(result);
        } catch (Exception e) {
            try {
                conn.rollback(savepoint);
            } catch (Exception nested) {
                e.addSuppressed(nested);
            }
            if (e instanceof LiquibaseException le) {
                throw le;
            } else {
                throw new DatabaseException(e);
            }
        }
    }

    @Override
    public <T> List<T> queryColumn(Database db,
                                   String comment,
                                   String query,
                                   Class<T> type) throws LiquibaseException {
        var conn = (JdbcConnection) db.getConnection();
        var executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", db);
        executor.comment(comment);

        Savepoint savepoint = conn.setSavepoint();
        try {
            @SuppressWarnings("deprecation")
            var sql = new RawSqlStatement(query);
            @SuppressWarnings("unchecked")
            var result = (List<T>) executor.queryForList(sql, type);
            conn.rollback(savepoint);
            return result;
        } catch (Exception e) {
            try {
                conn.rollback(savepoint);
            } catch (Exception nested) {
                e.addSuppressed(nested);
            }
            if (e instanceof LiquibaseException le) {
                throw le;
            } else {
                throw new DatabaseException(e);
            }
        }
    }

    @Override
    public <T> List<Map<String, T>> queryRows(Database db,
                                              String comment,
                                              String query) throws LiquibaseException {
        var conn = (JdbcConnection) db.getConnection();
        var executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", db);
        executor.comment(comment);

        Savepoint savepoint = conn.setSavepoint();
        try {
            @SuppressWarnings("deprecation")
            var sql = new RawSqlStatement(query);
            @SuppressWarnings("unchecked")
            var result = (List<Map<String, T>>) (List<?>) executor.queryForList(sql);
            conn.rollback(savepoint);
            return result;
        } catch (Exception e) {
            try {
                conn.rollback(savepoint);
            } catch (Exception nested) {
                e.addSuppressed(nested);
            }
            if (e instanceof LiquibaseException le) {
                throw le;
            } else {
                throw new DatabaseException(e);
            }
        }
    }

    public static final DatabaseSupport INSTANCE = new GenericDatabaseSupport("generic");

}
