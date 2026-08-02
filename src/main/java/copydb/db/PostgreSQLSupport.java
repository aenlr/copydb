package copydb.db;

import copydb.convert.BlobWriter;
import copydb.convert.ColumnReader;
import copydb.convert.ColumnWriter;
import copydb.convert.Converters;
import copydb.convert.NumberReader;
import copydb.convert.NumberWriter;
import liquibase.Scope;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Sequence;
import liquibase.structure.core.Table;
import org.postgresql.util.PGobject;

import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;

// TODO: handle triggers and stored procedures
public final class PostgreSQLSupport extends GenericDatabaseSupport {

    private PostgreSQLSupport() {
        super("postgresql");
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

        if ("oid".equals(targetType) || "blob".equals(targetType) || "lo".equals(targetType)) {
            return new BlobWriter();
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

    @Override
    public DdlCommitMode getDdlCommitMode() {
        return DdlCommitMode.TRANSACTIONAL;
    }

    @Override
    public void truncateTable(Database db, Table table) throws LiquibaseException {
        String name = db.escapeTableName(null, null, table.getName());
        String sql = "TRUNCATE TABLE " + name + " CASCADE";
        executeSql(db, sql);
    }

    @Override
    protected List<SqlStatement> alterSequenceStatements(Sequence source, Database db, Sequence target) {
        return alterSequenceRestart(source, db, target, value -> "RESTART WITH " + value);
    }

    @SuppressWarnings("deprecation")
    private boolean isSuperUser(Database db) throws LiquibaseException {
        var conn = (JdbcConnection) db.getConnection();
        var executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", db);
        executor.comment("Checking if superuser");

        Savepoint savepoint;
        Boolean superuser = false;
        try {
            savepoint = conn.getUnderlyingConnection().setSavepoint();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        try {
            superuser = executor.queryForObject(new RawSqlStatement("select rolsuper from pg_roles where rolname = current_role"), Boolean.class);
            conn.releaseSavepoint(savepoint);
        } catch (Exception e) {
            try {
                conn.getUnderlyingConnection().rollback(savepoint);
            } catch (SQLException nested) {
                throw new DatabaseException(e);
            }
        }

        return superuser != null && superuser;
    }

    @Override
    public Action disableForeignKeys(Database db, List<ForeignKey> keys) throws LiquibaseException {
        List<String> disableStmts = new ArrayList<>();
        List<String> enableStmts = new ArrayList<>();
        if (isSuperUser(db)) {
            disableStmts.add("SET session_replication_role = 'replica'");
            enableStmts.add("SET session_replication_role = 'origin'");
        } else {
            for (var fk : keys) {
                Table table = fk.getForeignKeyTable();
                if (!fk.isDeferrable()) {
                    String tableName = db.escapeTableName(null, null, table.getName());
                    String keyName = db.escapeConstraintName(fk.getName());
                    disableStmts.add("ALTER TABLE %s ALTER CONSTRAINT %s DEFERRABLE INITIALLY DEFERRED"
                        .formatted(tableName, keyName));
                    enableStmts.add("ALTER TABLE %s ALTER CONSTRAINT %s NOT DEFERRABLE"
                        .formatted(tableName, keyName));
                }
            }
            disableStmts.add("SET CONSTRAINTS ALL DEFERRED");
            enableStmts.add(0, "SET CONSTRAINTS ALL IMMEDIATE");
        }
        executeSql(db, disableStmts);
        return ExecuteSqlAction.sql(db, enableStmts);
    }

    public static final DatabaseSupport INSTANCE = new PostgreSQLSupport();
}
