package copydb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import static copydb.StringUtil.caseInsensitiveSet;
import static copydb.StringUtil.lowerCaseList;

public class ObjectFilter {

    private List<String> order = new ArrayList<>();
    private Set<String> include = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private Set<String> exclude = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private boolean enabled;

    public ObjectFilter(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<String> getOrder() {
        return order;
    }

    public void setOrder(List<String> order) {
        this.order = order != null ? lowerCaseList(order) : new ArrayList<>();
    }

    public Collection<String> getInclude() {
        return include;
    }

    public void setInclude(Collection<String> include) {
        if (include != null) {
            this.include = caseInsensitiveSet(include);
            this.order = include.stream().map(String::toLowerCase).toList();
        } else {
            this.include.clear();
            this.order.clear();
        }
    }

    public Collection<String> getExclude() {
        return exclude;
    }

    public void setExclude(Collection<String> exclude) {
        if (exclude != null) {
            this.exclude = caseInsensitiveSet(exclude);
        } else {
            this.exclude.clear();
        }
    }

    public boolean contains(String k) {
        if (!include.isEmpty() && include.contains(k)) {
            return true;
        }

        if (exclude.contains(k)) {
            return false;
        }

        return (include.isEmpty() && !exclude.contains("*")) || include.contains("*");
    }

    public <T> void sort(List<T> list, Function<T, String> keyExtractor) {
        if (order.isEmpty()) {
            return;
        }

        list.sort((a, b) -> {
            String ka = keyExtractor.apply(a).toLowerCase();
            String kb = keyExtractor.apply(b).toLowerCase();
            int ia = order.indexOf(ka);
            if (ia == -1) {
                ia = Integer.MAX_VALUE;
            }
            int ib = order.indexOf(kb);
            if (ib == -1) {
                ib = Integer.MAX_VALUE;
            }
            int cmp = Integer.compare(ia, ib);
            if (cmp == 0) {
                cmp = ka.compareTo(kb);
            }
            return cmp;
        });
    }

    public void load(PropertySource properties, String prefix) {
        enabled = StringUtil.parseBoolean(properties.getProperty(prefix + "enabled"), enabled);
        StringUtil.parseOptionalList(properties.getProperty(prefix + "include"))
            .ifPresent(this::setInclude);
        StringUtil.parseOptionalList(properties.getProperty(prefix + "order"))
            .ifPresent(this::setOrder);
        StringUtil.parseOptionalList(properties.getProperty(prefix + "exclude"))
            .ifPresent(this::setExclude);
    }

}
