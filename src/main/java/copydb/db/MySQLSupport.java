package copydb.db;

import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Table;

import java.util.List;

public class MySQLSupport extends GenericDatabaseSupport {

    private MySQLSupport() {
        this("mysql");
    }

    protected MySQLSupport(String shortName) {
        super(shortName);
    }

    @Override
    public void truncateTable(Database db, Table table) throws LiquibaseException {
        executeSql(db, "TRUNCATE " + db.escapeTableName(null, null, table.getName()));
    }

    @Override
    public Action disableForeignKeys(Database db, List<ForeignKey> keys) throws LiquibaseException {
        executeSql(db, "SET FOREIGN_KEY_CHECKS=0");
        return ExecuteSqlAction.sql(db, "SET FOREIGN_KEY_CHECKS=1");
    }

    @Override
    public DdlCommitMode getDdlCommitMode() {
        return DdlCommitMode.IMPLICIT_COMMIT;
    }

    @Override
    public DataCommitMode getDataCommitMode() {
        return DataCommitMode.BATCH;
    }

    public static final DatabaseSupport INSTANCE = new MySQLSupport();
}
