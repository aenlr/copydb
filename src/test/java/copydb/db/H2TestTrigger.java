package copydb.db;

import java.sql.Connection;
import java.sql.SQLException;

public class H2TestTrigger implements org.h2.api.Trigger {

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        newRow[0] = "FIRED";
    }

    @Override
    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type)
            throws SQLException {
        System.out.println(getClass().getName() + " initialized.");
    }

}
