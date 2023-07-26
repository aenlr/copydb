package copydb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public interface PropertySource {

    String getProperty(String property);

    default String getProperty(String name, String defaultValue) {
        return Optional.ofNullable(getProperty(name)).orElse(defaultValue);
    }

    default boolean getBoolean(String name, boolean defaultValue) {
        return StringUtil.parseBoolean(getProperty(name), defaultValue);
    }

    class Prefix implements PropertySource {
        private final String prefix;
        private final PropertySource delegatedProperties;

        public Prefix(String prefix, PropertySource delegatedProperties) {
            this.prefix = prefix;
            this.delegatedProperties = delegatedProperties;
        }

        @Override
        public String getProperty(String property) {
            return delegatedProperties.getProperty(prefix + property);
        }
    }

    class PropertiesSource implements PropertySource {

        private final Properties properties;

        public PropertiesSource(Properties properties) {
            this.properties = properties;
        }

        public PropertiesSource(Path path) {
            this.properties = new Properties(System.getProperties());
            if (Files.exists(path)) {
                try (var is = Files.newInputStream(path)) {
                    this.properties.load(is);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Invalid properties file", e);
                }
            }
        }

        @Override
        public String getProperty(String property) {
            return properties.getProperty(property);
        }
    }

    class MapProperties implements PropertySource {

        private final Map<String, String> map;

        public MapProperties(Map<String, String> map) {
            this.map = map;
        }

        @Override
        public String getProperty(String property) {
            return map.get(property);
        }
    }

    class Environment extends MapProperties {
        public Environment() {
            super(System.getenv());
        }

        public Environment(Map<String, String> map) {
            super(map);
        }

        @Override
        public String getProperty(String property) {
            var env = property.toUpperCase()
                .replaceAll("[^A-Z0-9_]+", "_");
            var val = super.getProperty(env);
            if (val == null) {
                env = property.replaceAll("[^a-zA-Z0-9_]+", "_");
                val = super.getProperty(env);
            }
            return val;
        }
    }

    class Aggregate implements PropertySource {

        private final List<PropertySource> propertySources;

        public Aggregate(PropertySource... propertySources) {
            this(Arrays.asList(propertySources));
        }

        public Aggregate(List<PropertySource> propertySources) {
            this.propertySources = propertySources;
        }

        public String getProperty(String name) {
            for (var source : propertySources) {
                String val = source.getProperty(name);
                if (val != null) {
                    return val;
                }
            }

            return null;
        }

    }

}
