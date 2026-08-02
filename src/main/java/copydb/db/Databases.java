package copydb.db;

import liquibase.database.Database;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class Databases {

    private Databases() {
    }

    private static final List<DatabaseSupport> DATABASES = List.of(
        H2Support.INSTANCE,
        MariaDBSupport.INSTANCE,
        MySQLSupport.INSTANCE,
        OracleSupport.INSTANCE,
        PostgreSQLSupport.INSTANCE
    );

    private static final Map<String, DatabaseSupport> DATABASES_BY_NAME = DATABASES.stream()
            .collect(Collectors.toMap(DatabaseSupport::getShortName, Function.identity()));

    public static DatabaseSupport getDatabase(String id) {
        return DATABASES_BY_NAME.getOrDefault(id, GenericDatabaseSupport.INSTANCE);
    }

    public static DatabaseSupport getDatabase(Database db) {
        return getDatabase(db.getShortName());
    }

}
