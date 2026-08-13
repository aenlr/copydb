package copydb.db;

import copydb.ObjectFilter;
import copydb.convert.ByteArrayWriter;
import copydb.convert.ColumnReader;
import copydb.convert.ColumnWriter;
import copydb.convert.NumberReader;
import copydb.convert.NumberWriter;
import copydb.convert.UUIDBytesWriter;
import liquibase.Scope;
import liquibase.database.Database;
import liquibase.database.PreparedStatementFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Sequence;
import liquibase.structure.core.Table;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;

public final class OracleSupport extends GenericDatabaseSupport {

    private OracleSupport() {
        super("oracle");
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

    private static final BigInteger SEQUENCE_MAX = new BigInteger("9999999999999999999999999999");

    @Override
    public BigInteger getSequenceMaxValue() {
        return SEQUENCE_MAX;
    }

    @Override
    protected List<SqlStatement> alterSequenceStatements(Sequence source, Database db, Sequence target) {
        return alterSequenceRestart(source, db, target, value -> "RESTART START WITH " + value);
    }

    @Override
    public DdlCommitMode getDdlCommitMode() {
        return DdlCommitMode.IMPLICIT_COMMIT;
    }

    @Override
    public void dropAll(Database db) throws LiquibaseException {
        super.dropAll(db);

        String dropJobs = """
            BEGIN
              -- Scheduled jobs
              FOR r IN (SELECT JOB_NAME FROM USER_SCHEDULER_JOBS)
              LOOP
                DBMS_SCHEDULER.DROP_JOB(r.JOB_NAME);
              END LOOP;
            END;""";

        String dropDomainIndexes = """
            BEGIN
              -- Domain indexes (context)
              FOR r IN (SELECT INDEX_NAME FROM USER_INDEXES WHERE INDEX_TYPE = 'DOMAIN')
              LOOP
                EXECUTE IMMEDIATE ('DROP INDEX "' || r.INDEX_NAME || '" FORCE');
              END LOOP;

              -- Context preferences
              FOR r in (SELECT PRE_NAME FROM CTX_PREFERENCES WHERE PRE_OWNER = (SELECT USER FROM DUAL))
              LOOP
                CTX_DDL.DROP_PREFERENCE(r.PRE_NAME);
              END LOOP;
            END;""";

        String dropProcedures = """
            BEGIN
              -- Stored procedures
              FOR r IN (SELECT OBJECT_NAME
                        FROM USER_PROCEDURES
                        WHERE OBJECT_TYPE = 'PROCEDURE')
              LOOP
                EXECUTE IMMEDIATE ('DROP PROCEDURE "' || r.OBJECT_NAME || '"');
              END LOOP;
            END;""";

        String dropTriggers = """
            BEGIN
              FOR r IN (SELECT TRIGGER_NAME
                          FROM USER_TRIGGERS
                         WHERE TABLE_NAME IS NOT NULL
                           AND TRIGGER_NAME NOT IN ('DDL_TRIGGER', 'TRUNCATE_TRIGGER'))
              LOOP
                EXECUTE IMMEDIATE ('DROP TRIGGER "' || r.TRIGGER_NAME || '"');
              END LOOP;
            END;""";

        String purgeRecycleBin = "PURGE RECYCLEBIN";
        executeSql(db, dropJobs, dropDomainIndexes, dropProcedures, dropTriggers, purgeRecycleBin);
    }

    @Override
    public void truncateTable(Database db, Table table) throws LiquibaseException {
        String name = db.escapeTableName(db.getDefaultCatalogName(), db.getDefaultSchemaName(), table.getName());
        String sql = "TRUNCATE TABLE %s DROP ALL STORAGE CASCADE".formatted(name);
        executeSql(db, sql);
    }

    private String toggleAllTriggersDdl(boolean enable) {
        return """
               BEGIN
                   FOR r IN (SELECT TRIGGER_NAME FROM USER_TRIGGERS)
                   LOOP
                       EXECUTE IMMEDIATE ('ALTER TRIGGER "' || r.TRIGGER_NAME || '" %s');
                   END LOOP;
               END;""".formatted(enable ? "ENABLE" : "DISABLE");
    }

    private String toggleTableTriggersDdl(Database db, boolean enable, List<Table> tables) {
        String inParams = tables.stream()
            .map(t -> db.escapeStringForDatabase(t.getName()))
            .collect(Collectors.joining(",", "(", ")"));

        return """
               BEGIN
                   FOR r IN (SELECT TRIGGER_NAME FROM USER_TRIGGERS WHERE TABLE_NAME IN %s)
                   LOOP
                       EXECUTE IMMEDIATE ('ALTER TRIGGER "' || r.TRIGGER_NAME || '" %s');
                   END LOOP;
               END;""".formatted(inParams, enable ? "ENABLE" : "DISABLE");
    }

    @Override
    public Action disableTriggers(Database db, ObjectFilter filter) throws LiquibaseException {
        if (filter.isEmpty()) {
            String disableDdl = toggleAllTriggersDdl(false);
            executeSql(db, disableDdl);
            return ExecuteSqlAction.sql(db, toggleAllTriggersDdl(true));
        }

        PreparedStatementFactory stmtFactory = new PreparedStatementFactory((JdbcConnection) db.getConnection());
        List<String> triggers = new ArrayList<>();
        try (var select = stmtFactory.create("SELECT DISTINCT TRIGGER_NAME, TABLE_NAME FROM USER_TRIGGERS ORDER BY 1, 2");
             var rs = select.executeQuery()) {
            while (rs.next()) {
                String trigger = rs.getString(1);
                String table = rs.getString(2);
                if ((table == null && filter.contains(trigger)) || (table != null && filter.contains(table, trigger))) {
                    triggers.add(trigger);
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        if (triggers.isEmpty()) {
            return Action.NOP;
        }

        executeSql(db, triggers.stream()
            .map(name -> "ALTER TRIGGER %s DISABLE".formatted(db.escapeConstraintName(name)))
            .toList());

        return ExecuteSqlAction.sql(db, triggers.stream()
            .map(name -> "ALTER TRIGGER %s ENABLE".formatted(db.escapeConstraintName(name)))
            .toList());
    }

    @Override
    public Action disableTriggers(Database db, List<Table> tables, ObjectFilter filter) throws LiquibaseException {
        if (filter.isEmpty()) {
            var disable = toggleTableTriggersDdl(db, false, tables);
            var enable = toggleTableTriggersDdl(db, true, tables);
            executeSql(db, disable);
            return ExecuteSqlAction.sql(db, enable);
        }

        PreparedStatementFactory stmtFactory = new PreparedStatementFactory((JdbcConnection) db.getConnection());
        List<String> triggers = new ArrayList<>();
        Set<String> tableNames = tables.stream().map(Table::getName).collect(Collectors.toSet());
        try (var select = stmtFactory.create("SELECT DISTINCT TRIGGER_NAME, TABLE_NAME FROM USER_TRIGGERS WHERE TABLE_NAME IS NOT NULL ORDER BY 1, 2");
             var rs = select.executeQuery()) {
            while (rs.next()) {
                String trigger = rs.getString(1);
                String table = rs.getString(2);
                if (!tableNames.contains(table)) {
                    continue;
                }
                if (filter.contains(table, trigger)) {
                    triggers.add(trigger);
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        if (triggers.isEmpty()) {
            return Action.NOP;
        }

        executeSql(db, triggers.stream()
            .map(name -> "ALTER TRIGGER %s DISABLE".formatted(db.escapeConstraintName(name)))
            .toList());

        return ExecuteSqlAction.sql(db, triggers.stream()
            .map(name -> "ALTER TRIGGER %s ENABLE".formatted(db.escapeConstraintName(name)))
            .toList());
    }

    @Override
    public Action disableForeignKeys(Database db, List<ForeignKey> keys) throws LiquibaseException {
        List<String> enable = new ArrayList<>(keys.size());
        List<String> disable = new ArrayList<>(keys.size());
        for (var fk : keys) {
            String table = db.escapeTableName(null, null, fk.getForeignKeyTable().getName());
            String constraint = db.escapeConstraintName(fk.getName());
            disable.add("ALTER TABLE %s DISABLE CONSTRAINT %s".formatted(table, constraint));
            enable.add("ALTER TABLE %s ENABLE CONSTRAINT %s".formatted(table, constraint));
        }

        executeSql(db, disable);
        return ExecuteSqlAction.sql(db, enable);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Trigger> getTriggers(Database db) throws LiquibaseException {
        var executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", db);
        var owner = db.getDefaultSchemaName(); // SELECT SYS_CONTEXT( 'USERENV', 'CURRENT_SCHEMA' ) FROM DUAL;
        executor.comment("Listing triggers");
        String allTriggersSql =
            """
            SELECT TRIGGER_NAME, STATUS, BASE_OBJECT_TYPE, TRIGGERING_EVENT, TABLE_OWNER, TABLE_NAME,
                   DESCRIPTION, TRIGGER_BODY,
                   (SELECT LISTAGG(SRC.TEXT) WITHIN GROUP (ORDER BY SRC.LINE)
                    FROM ALL_SOURCE SRC WHERE SRC.OWNER = TRG.OWNER AND SRC.NAME = TRG.TRIGGER_NAME AND SRC.TYPE = 'TRIGGER') MY_DDL
            FROM ALL_TRIGGERS TRG
            WHERE OWNER = ?
            ORDER BY TRG.TRIGGER_NAME""";
        var rows = (List<Map<String, String>>)(List<?>)executor.queryForList(new RawParameterizedSqlStatement(allTriggersSql, owner));
        var result = new ArrayList<Trigger>();
        for (var row : rows) {
            var name = row.get("TRIGGER_NAME");
            var ddl = row.get("MY_DDL");
            if (ddl != null) {
                ddl = "CREATE OR REPLACE " + ddl;
            } else {
                var description = row.get("DESCRIPTION");
                var body = row.get("TRIGGER_BODY");
                if (description == null || body == null) {
                    continue;
                }
                ddl = "CREATE OR REPLACE TRIGGER " + description + '\n' + body;
            }

            var trigger = new Trigger(null, owner, name);
            trigger.setDdl(ddl);
            trigger.setEnabled("ENABLED".equals(row.get("STATUS")));

            var baseObjectType = row.get("BASE_OBJECT_TYPE");
            if ("TABLE".equals(baseObjectType)) {
                var tableOwner = row.get("TABLE_OWNER");
                var tableName = row.get("TABLE_NAME");
                var table = new Table(null, tableOwner, tableName);
                trigger.setTable(table);
            }

            result.add(trigger);
        }

        return result;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void createTriggers(Database db, List<Trigger> triggers) throws LiquibaseException {
        List<SqlStatement> stmts = triggers.stream()
            .filter(t -> t.getDdl() != null)
            .flatMap(t -> Stream.<SqlStatement>of(
                new RawSqlStatement(t.getDdl(), "/"),
                new RawSqlStatement("ALTER TRIGGER %s %s".formatted(db.escapeIndexName(t.getSchema().getCatalogName(), t.getSchema().getName(), t.getName()), t.isEnabled() ? "ENABLE" : "DISABLE"))))
            .toList();
        execute(db, stmts);
    }

    public static final DatabaseSupport INSTANCE = new OracleSupport();

}
