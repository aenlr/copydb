package copydb.convert;

import liquibase.structure.core.Column;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;

public class NumberReader implements ColumnReader<Number> {

    @Override
    public Number convert(Column column, Object val) throws SQLException {
        if (val instanceof Number n) {
            return n;
        } else if (val instanceof String s) {
            if (s.indexOf('.') >= 0) {
                return new BigDecimal(s);
            } else {
                try {
                    return Long.valueOf(s, 10);
                } catch (NumberFormatException _ignored) {
                    return new BigInteger(s, 10);
                }
            }
        }

        throw new IllegalArgumentException("Cannot convert " + val + " to number");
    }

    static final ColumnReader<Number> INSTANCE = new NumberReader();

}
