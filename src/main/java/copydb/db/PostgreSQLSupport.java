package copydb.db;

import copydb.convert.BlobWriter;
import copydb.convert.ColumnReader;
import copydb.convert.ColumnWriter;
import copydb.convert.Converters;
import copydb.convert.NumberReader;
import copydb.convert.NumberWriter;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.statement.SqlStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Sequence;
import liquibase.structure.core.Table;
import org.postgresql.util.PGobject;

import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        entry("int8", NumberWriter.INSTANCE),
        entry("oid", BlobWriter.INSTANCE),
        entry("lo", BlobWriter.INSTANCE)
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

            switch (val) {
                case String s -> pgobj.setValue(s);
                case Clob clob -> pgobj.setValue(Converters.extractString(clob));
                case byte[] b -> pgobj.setValue(new String(b, StandardCharsets.UTF_8));
                case Blob blob -> pgobj.setValue(new String(Converters.extractBytes(blob), StandardCharsets.UTF_8));
                case null, default ->
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
    public void createDefaultSchemas(Database db) throws LiquibaseException {
        if (db.getDefaultSchemaName() == null && db.getLiquibaseSchemaName() == null) {
            return;
        }

        if (hasDatabasePrivilege(db, "create") || isSuperUser(db)) {
            if (db.getDefaultSchemaName() != null) {
                executeSql(db, "create schema if not exists " + db.getDefaultSchemaName());
            }
            if (db.getLiquibaseSchemaName() != null && !db.getLiquibaseSchemaName().equals(db.getDefaultSchemaName())) {
                executeSql(db, "create schema if not exists " + db.getLiquibaseSchemaName());
            }
        }
    }

    @Override
    public void dropAll(Database db) throws LiquibaseException {
        var schema = db.getDefaultSchemaName();
        if (schema != null) {
            if (hasDatabasePrivilege(db, "create") || isSuperUser(db)) {
                executeSql(db, "drop schema if exists " + schema + " cascade");
                vacuumLo(db);
                executeSql(db, "create schema " + schema);
                return;
            }
        }

        super.dropAll(db);
        vacuumLo(db);
    }

    private void vacuumLo(Database db) throws LiquibaseException {
        if (!hasColumnPrivilege(db, "pg_largeobject_metadata", "oid", "SELECT")) {
            return;
        }

        String predicates = queryLoColumns(db).stream()
            .map(col -> orphanedLoPredicate(db, col, "l.oid") + '\n')
            .collect(Collectors.joining("   AND "));
        if (predicates.isEmpty()) {
            executeSql(db, """
                SELECT lo_unlink(lom.oid)
                  FROM pg_largeobject_metadata lom
                  JOIN pg_roles r on r.oid = lomowner
                 WHERE r.rolname = current_role""");
        } else {
            executeSql(db, """
                SELECT lo_unlink(l.oid)
                  FROM pg_largeobject_metadata l
                 WHERE %s""".formatted(predicates));
        }
    }

    @Override
    public void truncateTable(Database db, Table table) throws LiquibaseException {
        boolean truncatePossible = true;
        for (var col : table.getColumns()) {
            String type = col.getType().getTypeName();
            if ("lo".equals(type) || "oid".equals(type)) {
                truncatePossible = false;
                break;
            }
        }

        if (truncatePossible) {
            String name = db.escapeTableName(null, null, table.getName());
            String sql = "TRUNCATE TABLE " + name + " CASCADE";
            executeSql(db, sql);
        } else {
            super.truncateTable(db, table);
        }
    }

    @Override
    protected List<SqlStatement> alterSequenceStatements(Sequence source, Database db, Sequence target) {
        return alterSequenceRestart(source, db, target, value -> "RESTART WITH " + value);
    }

    private static String orphanedLoPredicate(Database db, Column col, String ref) {
        String tableName = db.escapeTableName(col.getSchema().getCatalogName(), col.getSchema().getName(), col.getRelation().getName());
        String columnName = db.escapeColumnName(null, null, null, col.getName());
        return "NOT EXISTS (SELECT FROM " + tableName + " t WHERE t." + columnName + " = " + ref + ')';
    }

    private List<Column> queryLoColumns(Database db) throws LiquibaseException {
        var sql = """
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '%s' AND DATA_TYPE IN ('oid', 'lo')
            ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME""".formatted(
                db.escapeStringForDatabase(db.getDefaultSchemaName()));
        List<Map<String, String>> rows = queryRows(db, "Querying large object columns", sql);
        return rows.stream()
            .map(r -> new Column(Table.class, db.getDefaultCatalogName(),
                r.get("TABLE_SCHEMA"), r.get("TABLE_NAME"), r.get("COLUMN_NAME")))
            .toList();
    }

    private boolean hasDatabasePrivilege(Database db, String privilege) throws LiquibaseException {
        var sql = "select has_database_privilege(current_database(), '"
            + db.escapeStringForDatabase(privilege)
            + "')";
        return querySingle(db, "Query database privilege", sql, Boolean.class).orElse(false);
    }

    private boolean hasColumnPrivilege(Database db, String table, String column, String privilege) throws LiquibaseException {
        var sql = "select has_column_privilege(current_database(), '"
            + db.escapeStringForDatabase(table)
            + "', '"
            + db.escapeStringForDatabase(column)
            + "', '"
            + db.escapeStringForDatabase(privilege)
            + "')";
        return querySingle(db, "Query column privilege", sql, Boolean.class).orElse(false);
    }

    private Map<String, ?> queryPgRoles(Database db) throws LiquibaseException {
        List<Map<String, Object>> rows = queryRows(db, "Query pg_roles",
            "select * from pg_roles where rolname = current_role");
        if (rows.size() == 1) {
            return rows.getFirst();
        }

        return Map.of();
    }

    private boolean isSuperUser(Database db) throws LiquibaseException {
        var roles = queryPgRoles(db);
        Boolean superuser = (Boolean) roles.get("ROLSUPER");
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
            enableStmts.addFirst("SET CONSTRAINTS ALL IMMEDIATE");
        }
        executeSql(db, disableStmts);
        return ExecuteSqlAction.sql(db, enableStmts);
    }

    public static final DatabaseSupport INSTANCE = new PostgreSQLSupport();
}
