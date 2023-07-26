package copydb;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;

final class StringUtil {

    private StringUtil() {
    }

    static boolean hasLength(String str) {
        return str != null && !str.isEmpty();
    }

    static boolean isEmpty(String str) {
        return !hasLength(str);
    }

    static String trimToNull(String str) {
        return Optional.ofNullable(str)
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .orElse(null);
    }

    static Optional<List<String>> parseOptionalList(String s) {
        if (s == null) {
            return Optional.empty();
        }

        if (s.isBlank()) {
            return Optional.of(List.of());
        }

        var l = Arrays.stream(s.split("\\s*,\\s*"))
            .filter(e -> !e.isEmpty())
            .toList();
        return Optional.of(l);
    }

    static List<String> parseList(String s) {
        return parseOptionalList(s).orElse(List.of());
    }

    static List<String> parseList(String s, List<String> defaultValue) {
        return parseOptionalList(s).orElse(defaultValue);
    }

    static List<String> parseList(String s, Supplier<List<String>> defaultValue) {
        return parseOptionalList(s).orElseGet(defaultValue);
    }

    static Optional<Boolean> parseBoolean(String s) {
        if (s == null || s.isBlank()) {
            return Optional.empty();
        }

        s = s.toLowerCase(Locale.ROOT);
        if ("true".equals(s) || "yes".equals(s) || "on".equals(s) || "1".equals(s)) {
            return Optional.of(true);
        }

        if ("false".equals(s) || "no".equals(s) || "off".equals(s) || "0".equals(s)) {
            return Optional.of(false);
        }

        return Optional.empty();
    }

    static boolean parseBoolean(String s, boolean defaultValue) {
        return parseBoolean(s).orElse(defaultValue);
    }

    private static <T> Optional<T> parse(String s, Function<String, T> converter) {
        if (s == null || s.isBlank()) {
            return Optional.empty();
        }

        return Optional.ofNullable(converter.apply(s.trim()));
    }

    static Optional<Integer> parseInt(String s) {
        return parse(s, Integer::parseInt);
    }

    static int parseInt(String s, int defaultValue) {
        return parseInt(s).orElse(defaultValue);
    }

    static Set<String> caseInsensitiveSet(Collection<String> l) {
        Set<String> set = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        set.addAll(l);
        return set;
    }

    static List<String> lowerCaseList(Collection<String> l) {
        return l.stream()
            .map(String::toLowerCase)
            .toList();
    }
}
