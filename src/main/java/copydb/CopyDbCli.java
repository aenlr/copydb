package copydb;

import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;

import static copydb.StringUtil.parseBoolean;

public class CopyDbCli {

    static class ArgParser {
        final String[] args;
        int idx;
        String opt;
        String val;
        boolean flag;

        ArgParser(String[] args) {
            this.args = args;
        }

        public String current() {
            return args[idx];
        }

        public String next() {
            return args[idx++];
        }

        boolean hasNext() {
            return idx < args.length;
        }

        boolean arg(String name) throws CliException {
            opt = args[idx];
            String dashName = "-" + name;
            String ddashName = "-" + dashName;
            if (dashName.equals(opt) || ddashName.equals(opt)) {
                if (idx + 1 == args.length) {
                    throw new CliException(opt + ": argument required");
                }
                val = args[idx + 1];
                idx += 2;
                opt = name;
                return true;
            } else if (opt.startsWith(ddashName + "=")) {
                val = opt.substring(ddashName.length() + 1);
                idx++;
                opt = name;
                return true;
            }

            return false;
        }

        boolean flag(String name) throws CliException {
            String orgopt = args[idx];
            opt = orgopt;
            boolean invert = false;
            if (opt.startsWith("--no-")) {
                opt = "--" + opt.substring(5);
                invert = true;
            }

            String dashName = "--" + name;
            String ddashName = "-" + dashName;
            if (dashName.equals(opt) || ddashName.equals(opt)) {
                opt = name;
                if (idx + 1 < args.length) {
                    val = args[idx + 1].toLowerCase();
                    var b = parseBoolean(val);
                    if (b.isPresent()) {
                        idx += 2;
                        flag = invert != b.get();
                        return true;
                    }
                }

                flag = !invert;
                idx++;
                return true;
            } else if (opt.startsWith(ddashName + "=")) {
                val = opt.substring(ddashName.length() + 1);
                opt = name;
                var b = parseBoolean(val);
                if (b.isEmpty()) {
                    throw new CliException(orgopt + ": invalid boolean: " + val);
                }
                flag = invert != b.get();
                idx++;
                return true;
            }

            return false;
        }
    }

    static class CliException extends Exception {
        final boolean showHelp;

        public CliException(String message) {
            this(message, false);
        }

        public CliException(String message, boolean showHelp) {
            super(message);
            this.showHelp = showHelp;
        }
    }

    private static List<File> findDefaults(PropertySource config) {
        String userHome = System.getProperty("user.home");
        String configHome = System.getenv("XDG_CONFIG_HOME");
        if (configHome == null && userHome != null) {
            configHome = userHome + "/.config";
        }

        List<File> dirs = new ArrayList<>();

        if (configHome != null) {
            dirs.add(new File(configHome, "copydb"));
        }

        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            String appdata = System.getenv("APPDATA");
            if (appdata != null) {
                dirs.add(new File(appdata, "copydb"));
            }
        }

        if (userHome != null) {
            dirs.add(new File(userHome, ".copydb"));
        }

        List<File> files = new ArrayList<>();
        Optional.ofNullable(config.getProperty("config"))
            .map(File::new)
            .ifPresent(files::add);

        Optional.ofNullable(config.getProperty("secrets"))
            .map(File::new)
            .ifPresent(files::add);

        for (var d : dirs) {
            boolean found = false;
            var f = new File(d, "copydb.properties");
            if (f.exists()) {
                found = true;
                files.add(f);
            }

            f = new File(d, "secrets.properties");
            if (f.exists()) {
                found = true;
                files.add(f);
            }

            if (found) {
                break;
            }
        }

        return files;
    }

    static String getVersion() {
        try (var is = CopyDbCli.class.getResourceAsStream("/META-INF/maven/com.github.aenlr/copydb/pom.properties")) {
            if (is != null) {
                var props = new Properties();
                props.load(is);
                return props.getProperty("version", "unknown");
            }
        } catch (IOException ignored) {
        }
        return "unknown";
    }

    static void help(PrintStream os) {
        var version = getVersion();
        os.printf("""
            Syntax: copydb [OPTIONS]

            Version: %s
            """, version);

        os.println("""
            Examples:
            # Copy between two H2 databases, initializing target with liquibase.
            copydb --changelog=changelog.xml --truncate \\
              -u sa -p sa jdbc:h2:~/one \\
              -u sa -p sa jdbc:h2:~/two

            # Copy specified tables and sequences.
            # Same credentials for source and target
            copydb --truncate --tables footab,bartab --sequences fooseq,barseq \\
              -u sa -p sa jdbc:h2:~/one jdbc:h2:~/two

            # Copy between Oracle and H2 databases, initializing target with liquibase.
            # Passwords are read from environment variables.
            export COPYDB_SOURCE_PASSWORD=...
            export COPYDB_TARGET_PASSWORD=...
            copydb --changelog=changelog.xml --truncate \\
              -u ORAUSER jdbc:oracle:thin:@DBNAME -u H2USER jdbc:h2:~/two

            Connection:
              --source=URL            source database JDBC URL [COPYDB_SOURCE_URL]
              --target=URL            target database JDBC URL [COPYDB_TARGET_URL]
              -u, --username=USER     database user (specify twice)
                  --source-user=USER  [COPYDB_SOURCE_USERNAME]
                  --target-user=USER  [COPYDB_TARGET_USERNAME]
              -p, --password=PASS     database password (specify twice)
                  --source-pass=PASS  [COPYDB_SOURCE_PASSWORD]
                  --target-pass=PASS  [COPYDB_TARGET_PASSWORD]
              --init-sql=SQL          SQL to run after connecting to database [COPY_DB_INIT_SQL]
                                      Use file: or @filename notation to load from file.
              --post-sql=SQL          SQL to run after copying [COPY_DB_POST_SQL]
              --pre-copy-sql=SQL      SQL to run before copying data but after dropping
                                      database objects and running changelog
                                      [COPY_DB_PRE_COPY_SQL]

            URL specifies a jdbc-connection or a datasource reference in the
            form datasource:NAME.

            For datasources the connection URL and credentials are loaded from:
            - datasource.NAME.url
            - datasource.NAME.username
            - datasource.NAME.password

            Changelog:
              --changelog=FILE        execute liquibase changelog [COPYDB_CHANGELOG]
              --search-path=PATH      paths for looking up changelog resources
                                      [COPYDB_SEARCH_PATH]
              --contexts=EXPR         liquibase contexts expression [COPYDB_CONTEXTS]
              --label-filter=EXPR     liquibase label filter [COPYDB_LABEL_FILTER]
              --labels=EXPR
              --tag=TAG               tag to update to [COPYDB_TAG]
              --drop-first            drop ALL target database objects [COPYDB_DROP_FIRST]

            Tables:
              -t, --include=T1[,T2..] copy specific tables [COPYDB_TABLES_INCLUDE]
              -x, --exclude=T1[,T2..] exclude tables [COPYDB_TABLES_EXCLUDE]
                  --exclude-changelog exclude changelog tables
              -c, --column=T.C[,...]  include columns [COPYDB_COLUMNS_INCLUDE]
              -xc                     exclude columns [COPYDB_COLUMNS_EXCLUDE]
              --exclude-column=T.C[,...]
              --truncate              truncate target database tables [COPYDB_TRUNCATE]
              --disable-foreign-keys  disable foreign keys during copy (default: true)
                                      [COPYDB_DISABLE_FOREIGN_KEYS]

            Sequences:
              --copy-sequences        enable copying of sequences [COPYDB_SEQUENCES_ENABLED]
              -s, --seq S1[,S2]       copy specific sequences [COPYDB_SEQUENCES_INCLUDE]
                  --sequence S1[,S2]
              -xs S1                  exclude sequences [COPYDB_SEQUENCES_EXCLUDE]
              --exclude-sequence S

            General
              --properties=FILE       load settings from properties file
              --disable-triggers      disable triggers during copy (default: true)
                                      [COPYDB_DISABLE_TRIGGERS]

            Defaults are loaded from:
            - $COPYDB_CONFIG (or system property copydb.config)
            - $COPYDB_SECRETS (or system property copydb.secrets)
            - $HOME/.config/copydb/copydb.properties
            - $HOME/.config/copydb/secrets.properties
            - $APPDATA/copydb/copydb.properties (windows)
            - $APPDATA/copydb/secrets.properties (windows)
            """);
    }

    private static List<String> parseColumnList(String s) {
        if (s == null || s.isBlank()) {
            return List.of();
        }

        List<String> cols = new ArrayList<>();
        int len = s.length();
        int start = 0;
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c == ',' || Character.isWhitespace(c)) {
                if (i > start) {
                    String col = s.substring(start, i);
                    cols.add(col);
                }
                start = i + 1;
                continue;
            }

            if (c == '.' && i + 1 < len && s.charAt(i + 1) == '{') {
                String prefix = s.substring(start, i + 1);
                i += 2;
                while (i < len && Character.isWhitespace(s.charAt(i))) {
                    i++;
                }
                start = i;
                while (i < len && s.charAt(i) != '}') {
                    i++;
                }
                int right = i;
                while (right > start && Character.isWhitespace(s.charAt(right - 1))) {
                    right--;
                }
                for (String col : s.substring(start, right).split("\\s*,\\s*")) {
                    if (!col.isEmpty()) {
                        cols.add(prefix + col);
                    }
                }
                start = i + 1;
            }
        }

        if (start < len) {
            cols.add(s.substring(start, len));
        }

        return cols;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            help(System.out);
            System.exit(0);
        }

        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        System.setProperty("liquibase.fileEncoding", "UTF-8");
        System.setProperty("liquibase.secureParsing", "false");
        System.setProperty("liquibase.hub.mode", "off");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.liquibase.executor", "warn");

        var copier = new CopyDb();
        try {
            Properties commandLineArgs = new Properties();
            var propertySources = new ArrayList<PropertySource>();
            propertySources.add(new PropertySource.PropertiesSource(commandLineArgs));
            propertySources.add(new PropertySource.Prefix("copydb.", new PropertySource.PropertiesSource(System.getProperties())));
            propertySources.add(new PropertySource.Prefix("copydb.", new PropertySource.Environment()));
            var config = new PropertySource.Aggregate(propertySources);

            var configFiles = findDefaults(config);
            if (!configFiles.isEmpty()) {
                Properties defaults = new Properties();
                for (var f : configFiles) {
                    try (var is = Files.newInputStream(f.toPath())) {
                        defaults.load(is);
                    } catch (IOException e) {
                        throw new CliException(f + ": " + e.getMessage());
                    }
                }
                propertySources.add(new PropertySource.PropertiesSource(defaults));
            }

            var parser = new ArgParser(args);
            while (parser.hasNext()) {
                if (parser.arg("properties") || parser.arg("config")) {
                    try (var is = Files.newInputStream(Paths.get(parser.val))) {
                        commandLineArgs.load(is);
                    }
                } else if (parser.current().startsWith("-P")) {
                    var filename = parser.next().substring(2);
                    if (filename.isEmpty()) {
                        if (!parser.hasNext()) {
                            throw new CliException("-P requires an argument");
                        }
                        filename = parser.next();
                    }
                    try (var is = Files.newInputStream(Paths.get(filename))) {
                        commandLineArgs.load(is);
                    }
                } else if (parser.current().startsWith("-D")) {
                    var prop = parser.next().substring(2);
                    if (prop.isEmpty()) {
                        if (!parser.hasNext()) {
                            throw new CliException("-D requires an argument");
                        }
                        prop = parser.next();
                    }
                    var kv = prop.split("=", 2);
                    var val = kv.length == 2 ? kv[1] : "";
                    commandLineArgs.put(kv[0], val);
                } else if (parser.flag("version")) {
                    System.out.println(getVersion());
                    return;
                } else if (parser.arg("source") || parser.arg("source-url")) {
                    commandLineArgs.put("source.url", parser.val);
                } else if (parser.arg("target") || parser.arg("target-url")) {
                    commandLineArgs.put("target.url", parser.val);
                } else if (parser.arg("source-user") || parser.arg("source-username")) {
                    commandLineArgs.put("source.username", parser.val);
                } else if (parser.arg("source-pass") || parser.arg("source-password")) {
                    commandLineArgs.put("source.password", parser.val);
                } else if (parser.arg("target-user") || parser.arg("target-username")) {
                    commandLineArgs.put("target.username", parser.val);
                } else if (parser.arg("target-pass") || parser.arg("target-password")) {
                    commandLineArgs.put("target.password", parser.val);
                } else if (parser.current().startsWith("-c")) {
                    String opt = parser.next();
                    if (opt.length() == 2) {
                        if (!parser.hasNext()) {
                            throw new CliException("-c requires an argument");
                        }
                        opt = parser.next();
                    } else {
                        opt = opt.substring(2);
                    }

                    var l = new ArrayList<>(StringUtil.parseList(commandLineArgs.getProperty("columns.include")));
                    l.addAll(parseColumnList(opt));
                    commandLineArgs.put("columns.include", String.join(",", l));
                } else if (parser.arg("column") || parser.arg("columns") || parser.arg("col")) {
                    var l = new ArrayList<>(StringUtil.parseList(commandLineArgs.getProperty("columns.include")));
                    l.addAll(parseColumnList(parser.val));
                    commandLineArgs.put("columns.include", String.join(",", l));
                } else if (parser.arg("exclude-column") || parser.arg("exclude-columns")
                    || parser.arg("exclude-col") || parser.arg("xcol") || parser.arg("xc")
                    || parser.arg("excol") || parser.arg("ex-col")) {
                    var l = new ArrayList<>(StringUtil.parseList(commandLineArgs.getProperty("columns.exclude")));
                    l.addAll(parseColumnList(parser.val));
                    commandLineArgs.put("columns.exclude", String.join(",", l));
                    commandLineArgs.put("columns.enabled", "true");
                } else if (parser.arg("table") || parser.arg("tables") || parser.arg("include") || parser.arg("include-table") || parser.arg("include-tables")) {
                    var l = new ArrayList<>(StringUtil.parseList(commandLineArgs.getProperty("tables.include")));
                    l.add(parser.val);
                    commandLineArgs.put("tables.include", String.join(",", l));
                } else if (parser.current().startsWith("-t")) {
                    String opt = parser.next();
                    if (opt.length() == 2) {
                        if (!parser.hasNext()) {
                            throw new CliException("-t requires an argument");
                        }
                        opt = parser.next();
                    } else {
                        opt = opt.substring(2);
                    }

                    var l = new ArrayList<>(StringUtil.parseList(commandLineArgs.getProperty("tables.include")));
                    l.add(opt);
                    commandLineArgs.put("tables.include", String.join(",", l));
                } else if (parser.arg("exclude") || parser.arg("exclude-table") || parser.arg("exclude-tables")) {
                    var l = new ArrayList<>(StringUtil.parseList(commandLineArgs.getProperty("tables.exclude")));
                    l.add(parser.val);
                    commandLineArgs.put("tables.exclude", String.join(",", l));
                } else if (parser.flag("exclude-changelog")) {
                    var l = new ArrayList<>(StringUtil.parseList(commandLineArgs.getProperty("tables.exclude")));
                    l.addAll(Arrays.asList("DATABASECHANGELOG", "DATABASECHANGELOGLOCK"));
                    commandLineArgs.put("tables.exclude", String.join(",", l));
                } else if (parser.current().startsWith("-x")) {
                    String opt = parser.next();
                    if (opt.length() == 2) {
                        if (!parser.hasNext()) {
                            throw new CliException("-x requires an argument");
                        }
                        opt = parser.next();
                    } else {
                        opt = opt.substring(2);
                    }

                    var l = new ArrayList<>(StringUtil.parseList(commandLineArgs.getProperty("tables.exclude")));
                    l.add(opt);
                    commandLineArgs.put("tables.exclude", String.join(",", l));
                } else if (parser.flag("copy-tables")) {
                    commandLineArgs.put("tables.enabled", Boolean.toString(parser.flag));
                } else if (parser.arg("sequence") || parser.arg("sequences") || parser.arg("seq")) {
                    var l = new ArrayList<>(StringUtil.parseList(commandLineArgs.getProperty("sequences.include")));
                    l.add(parser.val);
                    commandLineArgs.put("sequences.include", String.join(",", l));
                    commandLineArgs.put("sequences.enabled", "true");
                } else if (parser.arg("exclude-sequence") || parser.arg("exclude-sequences")
                           || parser.arg("exclude-seq") || parser.arg("xseq") || parser.arg("xs")
                           || parser.arg("exseq") || parser.arg("ex-seq")) {
                    var l = new ArrayList<>(StringUtil.parseList(commandLineArgs.getProperty("sequences.exclude")));
                    l.add(parser.val);
                    commandLineArgs.put("sequences.exclude", String.join(",", l));
                    commandLineArgs.put("sequences.enabled", "true");
                } else if (parser.flag("copy-sequences") || parser.flag("copy-seq")) {
                    commandLineArgs.put("sequences.enabled", Boolean.toString(parser.flag));
                } else if (parser.flag("truncate") || parser.flag("drop-first")) {
                    commandLineArgs.put(parser.opt, Boolean.toString(parser.flag));
                } else if (parser.arg("batch-size") || parser.arg("batch")) {
                    commandLineArgs.put("batch-size", parser.val);
                } else if (parser.arg("changelog") || parser.arg("changelog-file")) {
                    commandLineArgs.put("changelog", parser.val);
                } else if (parser.arg("classpath") || parser.arg("class-path") || parser.arg("cp")) {
                    commandLineArgs.put("classpath", parser.val);
                } else if (parser.arg("search-path")) {
                    commandLineArgs.put("search-path", parser.val);
                } else if (parser.arg("label-filter") || parser.arg("contexts") || parser.arg("tag")) {
                    commandLineArgs.put(parser.opt, parser.val);
                } else if (parser.arg("context")) {
                    commandLineArgs.put("contexts", parser.val);
                } else if (parser.arg("labels") || parser.arg("label")) {
                    commandLineArgs.put("label-filter", parser.val);
                } else if (parser.flag("disable-foreign-keys") || parser.flag("disable-foreign-key")
                           || parser.flag("disable-fks") || parser.flag("disable-fk")
                           || parser.flag("disable-foreignkeys") || parser.flag("disable-foreignkey")) {
                    commandLineArgs.put("disable-foreign-keys", Boolean.toString(parser.flag));
                } else if (parser.flag("disable-triggers") || parser.flag("disable-trigger")
                    || parser.flag("no-triggers") || parser.flag("no-trigger")) {
                    commandLineArgs.put("disable-triggers", Boolean.toString(parser.flag));
                } else if (parser.arg("init-sql") || parser.arg("post-sql") || parser.arg("pre-copy-sql")) {
                    commandLineArgs.put(parser.opt, parser.val);
                } else if (parser.current().startsWith("-u")) {
                    String user;
                    String opt = parser.next();
                    if (opt.length() == 2) {
                        if (!parser.hasNext()) {
                            throw new CliException("-u requires an argument");
                        }
                        user = parser.next();
                    } else {
                        user = opt.substring(2);
                    }
                    if (commandLineArgs.getProperty("source.username") == null) {
                        commandLineArgs.put("source.username", user);
                    } else if (commandLineArgs.getProperty("target.username") == null) {
                        commandLineArgs.put("target.username", user);
                    } else {
                        throw new CliException("too many -u/--username options: " + parser.val);
                    }
                } else if (parser.arg("username")) {
                    if (commandLineArgs.getProperty("source.username") == null) {
                        commandLineArgs.put("source.username", parser.val);
                    } else if (commandLineArgs.getProperty("target.username") == null) {
                        commandLineArgs.put("target.username", parser.val);
                    } else {
                        throw new CliException("too many --username options: " + parser.val);
                    }
                } else if (parser.arg("password")) {
                    if (commandLineArgs.getProperty("source.password") == null) {
                        commandLineArgs.put("source.password", parser.val);
                    } else if (commandLineArgs.getProperty("target.password") == null) {
                        commandLineArgs.put("target.password", parser.val);
                    } else {
                        throw new CliException("too many --password options: " + parser.val);
                    }
                } else if (parser.current().startsWith("-p")) {
                    String pass;
                    String opt = parser.next();
                    if (opt.length() == 2) {
                        if (!parser.hasNext()) {
                            throw new CliException("-p requires an argument");
                        }
                        pass = parser.next();
                    } else {
                        pass = opt.substring(2);
                    }
                    if (commandLineArgs.getProperty("source.password") == null) {
                        commandLineArgs.put("source.password", pass);
                    } else if (commandLineArgs.getProperty("target.password") == null) {
                        commandLineArgs.put("target.password", pass);
                    } else {
                        throw new CliException("too many -p/--password options: " + parser.val);
                    }
                } else if (parser.current().startsWith("jdbc:") || parser.current().startsWith("datasource:")) {
                    if (commandLineArgs.getProperty("source.url") == null) {
                        commandLineArgs.put("source.url", parser.next());
                    } else if (commandLineArgs.getProperty("target.url") == null) {
                        commandLineArgs.put("target.url", parser.next());
                    } else {
                        throw new CliException("too many database URLs: " + parser.current());
                    }
                } else if ("--help" .equals(parser.current())
                           || "-h" .equals(parser.current())
                           || "-?" .equals(parser.current())) {
                    help(System.out);
                    System.exit(0);
                } else if (parser.current().startsWith("-")) {
                    throw new CliException("invalid option: " + parser.current(), true);
                } else {
                    throw new CliException("too many arguments: " + parser.current(), true);
                }
            }

            try {
                copier.load(config);
            } catch (NoSuchElementException e) {
                throw new CliException("Property missing: " + e.getMessage());
            }

            var source = copier.getSource();
            var target = copier.getTarget();

            if (source.getUrl() == null || target.getUrl() == null) {
                throw new CliException("Source and target URL:s not specified");
            }

            if (source.getUsername() == null && target.getUsername() != null) {
                source.setUsername(target.getUsername());
            } else if (source.getUsername() == null) {
                throw new CliException("Source database user not specified");
            }

            if (source.getPassword() == null) {
                var console = System.console();
                if (console == null) {
                    throw new CliException("Source database password not specified");
                }

                var psw = console.readPassword("Source database password: ");
                if (psw != null) {
                    source.setPassword(new String(psw));
                }
            }

            if (target.getUsername() == null) {
                target.setUsername(source.getUsername());
                if (target.getPassword() == null) {
                    target.setPassword(source.getPassword());
                }
            } else if (target.getPassword() == null) {
                var console = System.console();
                if (console == null) {
                    throw new CliException("Target database password not specified");
                }

                var psw = console.readPassword("Target database password: ");
                if (psw != null) {
                    target.setPassword(new String(psw));
                }
            }

        } catch (CliException e) {
            System.err.println("copydb: " + e.getMessage());
            if (e.showHelp) {
                help(System.err);
            }
            System.exit(1);
        }

        long start = System.currentTimeMillis();
        copier.copy();
        long end = System.currentTimeMillis();
        CopyDb.LOG.info("Finished ({})", String.format("%.2f s", (end - start) / 1000.0));
    }

}
