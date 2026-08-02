package copydb.db;

import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.statement.SqlStatement;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Sequence;
import liquibase.structure.core.Table;

import java.util.List;

// TODO: handle triggers
public final class H2Support extends GenericDatabaseSupport {

    private H2Support() {
        super("h2");
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

    public static final DatabaseSupport INSTANCE = new H2Support();
}
