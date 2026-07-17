package copydb.convert;

import liquibase.structure.core.Column;

public class BooleanWriter implements ColumnWriter<Boolean> {

    @Override
    public Boolean convert(Column column, Object val) {
        if (val instanceof String s) {
            if (s.length() == 1) {
                char c = s.charAt(0);
                if (c == 'Y' || c == 'y' || c == 'J' || c == 'j' || c == '1') {
                    return true;
                }
                if (c == 'N' || c == 'n' || c == '0') {
                    return false;
                }
            }
        } else if (val instanceof Number n) {
            return n.longValue() != 0;
        } else if (val instanceof Boolean b) {
            return b;
        }

        throw new IllegalArgumentException("Cannot convert " + val + " to " + column);
    }

    static final BooleanWriter INSTANCE = new BooleanWriter();

}
