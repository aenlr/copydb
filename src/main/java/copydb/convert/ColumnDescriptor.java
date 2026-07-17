package copydb.convert;

import liquibase.structure.core.Column;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnDescriptor<S,T> {

    private final Column source;
    private final Column target;
    private final ColumnReader<S> reader;
    private final ColumnWriter<T> writer;

    public ColumnDescriptor(Column source, Column target, ColumnReader<S> reader, ColumnWriter<T> writer) {
        this.source = source;
        this.target = target;
        this.reader = reader;
        this.writer = writer;
    }

    public T copy(ResultSet rs, PreparedStatement stmt, int paramIndex) throws SQLException {
        S src = reader.get(source, rs);
        if (src == null) {
            stmt.setObject(paramIndex, null);
            return null;
        }

        return writer.write(target, src, stmt, paramIndex);
    }

    @Override
    public String toString() {
        return source.getName() + " " + source.getType() + " -> " + target.getName() + " " + target.getType() + " [" + reader.getClass().getSimpleName() + ":" + writer.getClass().getSimpleName() + "]";
    }
}
