package copydb.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import liquibase.Scope;
import liquibase.database.core.H2Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.core.RawParameterizedSqlStatement;

class H2Test {

    @Test
    void testDropAll() throws Exception {
        // Test that all objects that we manage are dropped.
        // H2Support.dropAll uses DROP ALL OBJECT which doesn't
        // require any introspection like Liquibase.
        try (var db = openDatabase()) {
            var h2 = H2Support.INSTANCE;
            h2.executeSql(db, "CREATE TABLE TB(ID BIGINT PRIMARY KEY)");
            h2.executeSql(db, "CREATE SEQUENCE TSEQ START WITH 1 INCREMENT BY 10");
            h2.executeSql(db, "CREATE VIEW TV AS SELECT * FROM INFORMATION_SCHEMA.SEQUENCES");
            h2.executeSql(db, "CREATE TRIGGER TRG BEFORE INSERT ON TB FOR EACH ROW CALL '" + H2TestTrigger.class.getName() + "'");
            assertTrue(tableExists(db, "TB"));
            assertTrue(sequenceExists(db, "TSEQ"));
            assertTrue(viewExists(db, "TV"));
            assertTrue(triggerExists(db, "TRG"));
            h2.dropAll(db);
            assertFalse(tableExists(db, "TB"));
            assertFalse(sequenceExists(db, "TSEQ"));
            assertFalse(viewExists(db, "TV"));
            assertFalse(triggerExists(db, "TRG"));
        }
    }

    @Test
    void testH2Trigger() throws Exception {
        try (var src = openDatabase();
             var tgt = openDatabase()) {
            var h2 = H2Support.INSTANCE;
            String createTab = "CREATE TABLE TAB(DATA VARCHAR(255))";
            h2.executeSql(src, createTab);
            h2.executeSql(src, "CREATE TRIGGER TRG BEFORE INSERT ON TAB FOR EACH ROW CALL '" + H2TestTrigger.class.getName() + "'");
            var srcTriggers = h2.getTriggers(src);
            assertEquals(1, srcTriggers.size());

            h2.executeSql(src, "DROP TRIGGER TRG");
            assertEquals(0, h2.getTriggers(src).size());

            h2.executeSql(src, "INSERT INTO TAB(DATA) VALUES('X')");
            src.commit();
            var rows = queryTable(src, "TAB");
            assertEquals(1, rows.size());
            assertEquals("X", rows.get(0).get("DATA"));

            h2.executeSql(tgt, createTab);
            assertTrue(h2.canCopyTriggers(src, tgt));
            h2.createTriggers(tgt, srcTriggers);

            h2.executeSql(tgt, "INSERT INTO TAB(DATA) VALUES('X')");
            tgt.commit();
            rows = queryTable(tgt, "TAB");
            assertEquals(1, rows.size());
            assertEquals("FIRED", rows.get(0).get("DATA"));
        }
    }

    private static List<Map<String, ?>> queryTable(H2Database db, String tableName) throws DatabaseException {
        var executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", db);
        return executor.queryForList(new RawParameterizedSqlStatement("SELECT * FROM " + tableName));
    }

    private static List<Map<String, ?>> queryInformationSchema(H2Database db, String infoTable, String schemaColumn, String nameColumn, String name) throws DatabaseException {
        var sql = "SELECT * FROM INFORMATION_SCHEMA.%s WHERE %s = 'PUBLIC' AND %s = ?"
            .formatted(infoTable, schemaColumn, nameColumn, name);
        var executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", db);
        return executor.queryForList(new RawParameterizedSqlStatement(sql, name));
    }

    private static List<Map<String, ?>> queryInformationSchema(H2Database db, String infoTable, String prefix, String name) throws DatabaseException {
        return queryInformationSchema(db, infoTable, prefix + "_SCHEMA", prefix + "_NAME", name);
    }

    private static boolean sequenceExists(H2Database db, String name) throws DatabaseException {
        return !queryInformationSchema(db, "SEQUENCES", "SEQUENCE", name).isEmpty();
    }

    private static boolean tableExists(H2Database db, String name) throws DatabaseException {
        return !queryInformationSchema(db, "TABLES", "TABLE", name).isEmpty();
    }

    private static boolean viewExists(H2Database db, String name) throws DatabaseException {
        return !queryInformationSchema(db, "VIEWS", "TABLE_SCHEMA", "TABLE_NAME", name).isEmpty();
    }

    private static boolean triggerExists(H2Database db, String name) throws DatabaseException {
        return !queryInformationSchema(db, "TRIGGERS", "TRIGGER", name).isEmpty();
    }

    private H2Database openDatabase() throws SQLException {
        var driver = new org.h2.Driver();
        var conn = driver.connect("jdbc:h2:mem:" + Math.random(), null);
        var db = new H2Database();
        db.setConnection(new JdbcConnection(conn));
        return db;
    }

}
