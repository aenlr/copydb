package copydb.db;

public final class MariaDBSupport extends MySQLSupport {

    public MariaDBSupport() {
        super("mariadb");
    }

    public static final DatabaseSupport INSTANCE = new MariaDBSupport();

}
