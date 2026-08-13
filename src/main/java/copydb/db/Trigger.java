package copydb.db;

import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;

public class Trigger {

    private String name;
    private Schema schema;
    private Table table;
    private String ddl;
    private boolean enabled;

    public Trigger(String catalogName, String schemaName, String triggerName) {
        setSchema(new Schema(catalogName, schemaName));
        setName(triggerName);
        enabled = true;
    }

    @Override
    public String toString() {
        return getName();
    }


    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }


}
