package copydb.convert;

import liquibase.database.Database;
import liquibase.structure.core.Column;

public interface DatabaseSupport {

    ColumnReader<?> readerFor(Database source, Column sourceColumn);
    ColumnWriter<?> writerFor(Database target, Column targetColumn);

}
