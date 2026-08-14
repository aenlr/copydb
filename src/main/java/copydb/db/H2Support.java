package copydb.db;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Sequence;
import liquibase.structure.core.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class H2Support extends GenericDatabaseSupport {

    private H2Support() {
        super("h2");
    }

    @Override
    public void dropAll(Database db) throws LiquibaseException {
        executeSql(db, "DROP ALL OBJECTS");
    }

    @Override
    public void truncateTable(Database db, Table table) throws LiquibaseException {
        executeSql(db, "TRUNCATE TABLE " + db.escapeTableName(null, null, table.getName()));
    }

    @Override
    public Action disableForeignKeys(Database db, List<ForeignKey> keys) throws LiquibaseException {
        executeSql(db, "SET REFERENTIAL_INTEGRITY FALSE");
        return ExecuteSqlAction.sql(db, "SET REFERENTIAL_INTEGRITY TRUE");
    }

    @Override
    protected List<SqlStatement> alterSequenceStatements(Sequence source, Database db, Sequence target) {
        return alterSequenceRestart(source, db, target, value -> "RESTART WITH " + value);
    }

    private static final Pattern CREATE_TRIGGER_PATTERN = Pattern.compile("^(CREATE .*\\bTRIGGER )\"[^\"]+\"\\.\"[^\"]+\"");

    @SuppressWarnings("deprecation")
    @Override
    public List<Trigger> getTriggers(Database db) throws LiquibaseException {
        String sql = """
                SELECT TRIGGER_CATALOG, TRIGGER_SCHEMA, TRIGGER_NAME,
                EVENT_OBJECT_CATALOG, EVENT_OBJECT_SCHEMA, EVENT_OBJECT_TABLE,
                DB_OBJECT_SQL('TRIGGER', TRIGGER_SCHEMA, TRIGGER_NAME) AS DDL
                FROM INFORMATION_SCHEMA.TRIGGERS
                WHERE TRIGGER_SCHEMA = CURRENT_SCHEMA""";

        var executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", db);
        @SuppressWarnings("unchecked")
        var rows = (List<Map<String, String>>) (List<?>) executor.queryForList(new RawSqlStatement(sql));
        var result = new ArrayList<Trigger>();
        for (var row : rows) {
            var name = row.get("TRIGGER_NAME");
            var catalog = row.get("TRIGGER_CATALOG");
            var schema = row.get("TRIGGER_SCHEMA");
            var ddl = row.get("DDL");
            var m = CREATE_TRIGGER_PATTERN.matcher(ddl);
            if (m.find()) {
                StringBuilder sb = new StringBuilder();
                m.appendReplacement(sb, "$1");
                sb.append('"').append(name).append('"');
                m.appendTail(sb);
                ddl = sb.toString();
            } else {
                throw new DatabaseException("Unsupported H2 trigger DDL: " + ddl);
            }

            // CREATE FORCE TRIGGER "PUBLIC"."TRG"
            // BEFORE INSERT ON "PUBLIC"."TAB" FOR EACH ROW QUEUE 1024 CALL 'copydb.db.H2TestTrigger'
            var trigger = new Trigger(db.getDefaultCatalogName(), schema, name);
            trigger.setDdl(ddl);
            trigger.setSchema(new Schema(catalog, schema));
            String tableName = row.get("EVENT_OBJECT_TABLE");
            if (tableName != null) {
                String tableCatalog = row.get("EVENT_OBJECT_CATALOG");
                String tableSchema = row.get("EVENT_OBJECT_SCHEMA");
                trigger.setTable(new Table(tableCatalog, tableSchema, tableName));
            }

            result.add(trigger);
        }

        return result;
    }

    public static final DatabaseSupport INSTANCE = new H2Support();
}
