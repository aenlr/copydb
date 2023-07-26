package copydb;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static copydb.StringUtil.trimToNull;

public class JdbcProperties {
    private String username;
    private String password;
    private String url;
    private boolean readonly;
    private String initSql;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = trimToNull(username);
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = trimToNull(password);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        url = trimToNull(url);
        if (url != null) {
            URI uri = URI.create(url);
            if (uri.getUserInfo() != null) {
                String[] parts = uri.getUserInfo().split(":", 2);
                this.username = parts[0];
                if (parts.length == 2) {
                    this.password = parts[1];
                }
                try {
                    url = new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getRawPath(), uri.getRawQuery(), uri.getRawFragment()).toString();
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        }
        this.url = url;
    }

    public boolean isReadonly() {
        return readonly;
    }

    public JdbcProperties setReadonly(boolean readonly) {
        this.readonly = readonly;
        return this;
    }

    public String getInitSql() {
        return initSql;
    }

    public JdbcProperties setInitSql(String initSql) {
        this.initSql = initSql;
        return this;
    }

    private String getDbProperty(PropertySource properties,
                                 String connectionPrefix,
                                 String datasourcePrefix,
                                 String key,
                                 String defaultValue) {
        List<String> keys = new ArrayList<>(3);
        keys.add(connectionPrefix + key);

        if (datasourcePrefix != null) {
            keys.add(datasourcePrefix + key);
        }

        for (var k : keys) {
            String val = properties.getProperty(k);
            if (val != null) {
                return val;
            }
        }

        return defaultValue;
    }

    public void load(PropertySource properties, String prefix) {
        String url = properties.getProperty(prefix + "url");
        String datasourcePrefix = null;
        if (url != null) {
            if (url.startsWith("datasource:")) {
                String datasource = url.substring(11);
                datasourcePrefix = "datasource." + datasource + ".";
                String urlKey = datasourcePrefix + "url";
                url = properties.getProperty(urlKey);
                if (url == null) {
                    throw new NoSuchElementException(urlKey);
                }
            }
            setUrl(url);
        }

        setUsername(getDbProperty(properties, prefix, datasourcePrefix, "username", this.username));
        setPassword(getDbProperty(properties, prefix, datasourcePrefix, "password", this.password));
        setInitSql(getDbProperty(properties, prefix, datasourcePrefix, "init-sql", this.initSql));
        setReadonly(StringUtil.parseBoolean(getDbProperty(properties, prefix, datasourcePrefix, "readonly", null), this.readonly));
    }

}
