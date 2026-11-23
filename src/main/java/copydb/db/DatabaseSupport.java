package copydb.db;

import copydb.ObjectFilter;
import copydb.convert.ColumnReader;
import copydb.convert.ColumnWriter;
import liquibase.CatalogAndSchema;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Sequence;
import liquibase.structure.core.Table;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public interface DatabaseSupport {

    /**
     * Get liquibase database short name.
     */
    String getShortName();

    /**
     * Get a reader for a column.
     */
    ColumnReader<?> readerFor(Database source, Column sourceColumn);

    /**
     * Get a suitable writer for a column.
     */
    ColumnWriter<?> writerFor(Database target, Column targetColumn);

    /**
     * Get the largest possible max value for a sequence.
     */
    BigInteger getSequenceMaxValue();

    /**
     * Copy a sequence by altering an existing sequence or creating a new sequence.
     */
    void copySequence(Sequence source, Database db, Sequence target) throws LiquibaseException;

    /**
     * Enumerate triggers in database.
     */
    List<Trigger> getTriggers(Database db) throws LiquibaseException;

    <T> Optional<T> querySingle(Database db,
                                String comment,
                                String query,
                                Class<T> clazz) throws LiquibaseException;

    <T> Optional<T> queryOptional(Database db,
                                  String comment,
                                  String query,
                                  Class<T> clazz) throws LiquibaseException;

    <T> List<T> queryColumn(Database db,
                            String comment,
                            String query,
                            Class<T> type) throws LiquibaseException;

    <T> List<Map<String, T>> queryRows(Database db,
                                       String comment,
                                       String query) throws LiquibaseException;

    enum DdlCommitMode {
        /**
         * Executing DDL will implicitly commit.
         */
        IMPLICIT_COMMIT,
        /**
         * Any work in a transaction must be commited explicitly before executing DDL.
         */
        REQUIRE_COMMIT,
        /**
         * DDL is transactional and joins the current transaction.
         */
        TRANSACTIONAL
    }

    /**
     * Get DDL transaction characteristics.
     */
    default DdlCommitMode getDdlCommitMode() {
        return DdlCommitMode.REQUIRE_COMMIT;
    }

    /**
     * Get default data batch size.
     */
    default int getDefaultBatchSize() {
        return 500;
    }

    enum DataCommitMode {
        /**
         * Commit once after all data has been copied.
         */
        ONCE,
        /**
         * Commit after each batch.
         */
        BATCH,
        /**
         * Commit after each table.
         */
        TABLE,
    }

    /**
     * Get data commit mode.
     */
    default DataCommitMode getDataCommitMode() {
        return DataCommitMode.ONCE;
    }

    /**
     * Create default and liquibase schemas.
     * This is mostly for database types that allow users to create schemas.
     * No exception is thrown if the database does not support it or if the user
     * lacks permission to create schemas.
     * @param db target database
     * @throws LiquibaseException
     */
    default void createDefaultSchemas(Database db) throws LiquibaseException {
    }

    /**
     * Drop all database objects.
     * @param db target database
     * @throws LiquibaseException
     */
    default void dropAll(Database db) throws LiquibaseException {
        var where = new CatalogAndSchema(db.getDefaultCatalogName(), db.getDefaultSchemaName());
        db.dropDatabaseObjects(where);
    }

    /**
     * Truncate a table.
     * <p>The default implementation simply executes a DELETE</p>
     * @param db target database
     * @param table table to truncate
     * @throws LiquibaseException
     */
    default void truncateTable(Database db, Table table) throws LiquibaseException {
        executeSql(db, "DELETE FROM " + db.escapeTableName(null, null, table.getName()));
    }

    @FunctionalInterface
    interface Action {
        void execute() throws LiquibaseException;

        Action NOP = () -> {};
    }

    class ExecuteSqlAction implements Action {
        private final Database db;
        private final SqlStatement[] stmts;

        private ExecuteSqlAction(Database db, SqlStatement[] stmts) {
            this.db = db;
            this.stmts = stmts;
        }

        public static ExecuteSqlAction statements(Database db, SqlStatement... statements) {
            return new ExecuteSqlAction(db, statements);
        }

        public static ExecuteSqlAction statements(Database db, List<? extends SqlStatement> statements) {
            return new ExecuteSqlAction(db, statements.toArray(SqlStatement[]::new));
        }

        public static ExecuteSqlAction sql(Database db, String... sql) {
            return sql(db, Arrays.asList(sql));
        }

        @SuppressWarnings("deprecation")
        public static ExecuteSqlAction sql(Database db, Collection<String> sql) {
            // Not RawParameterizedSqlStatement since it bypasses logging listener
            return new ExecuteSqlAction(db, sql.stream()
                .map(RawSqlStatement::new)
                .toArray(SqlStatement[]::new));
        }

        @Override
        public void execute() throws LiquibaseException {
            db.execute(stmts, List.of());
        }
    }

    /**
     * Disable all triggers.
     *
     * @param db target database
     * @param filter trigger filter
     * @return action that when executed re-enables disabled triggers
     */
    Action disableTriggers(Database db, ObjectFilter filter) throws LiquibaseException;

    /**
     * Disable triggers for specific tables.
     *
     * @param db target database
     * @param tables tables whose triggers should be disabled
     * @param filter trigger filter
     * @return action that when executed re-enables disabled triggers
     */
    Action disableTriggers(Database db, List<Table> tables, ObjectFilter filter) throws LiquibaseException;

    /**
     * Disable foreign keys.
     *
     * @param db target database
     * @param keys keys to disable
     * @return action that when executed re-enables disabled foreign keys
     */
    Action disableForeignKeys(Database db, List<ForeignKey> keys) throws LiquibaseException;

    @SuppressWarnings("deprecation")
    default void executeSql(Database db, List<String> sql) throws LiquibaseException {
        // Not RawParameterizedSqlStatement since it bypasses logging listener
        SqlStatement[] statements = sql.stream()
            .map(RawSqlStatement::new)
            .toArray(SqlStatement[]::new);
        execute(db, statements);
    }

    default void executeSql(Database db, String... sql) throws LiquibaseException {
        executeSql(db, Arrays.asList(sql));
    }

    default void execute(Database db, List<? extends SqlStatement> statements) throws LiquibaseException {
        execute(db, statements.toArray(SqlStatement[]::new));
    }

    default void execute(Database db, SqlStatement... statements) throws LiquibaseException {
        db.execute(statements, List.of());
    }

    default boolean canCopyTriggers(Database source, Database target) {
        return source.getClass().equals(target.getClass());
    }

    default void createTriggers(Database db, List<Trigger> triggers) throws LiquibaseException {
        var stmts = triggers.stream()
            .map(Trigger::getDdl)
            .filter(Objects::nonNull)
            .toList();
        executeSql(db, stmts);
    }

}
