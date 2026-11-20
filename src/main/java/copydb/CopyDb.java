package copydb;

import copydb.convert.ColumnDescriptor;
import copydb.convert.Converters;
import copydb.db.DatabaseSupport;
import copydb.db.Databases;
import copydb.util.CopyTableProgress;
import liquibase.CatalogAndSchema;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.Scope;
import liquibase.Scope.ScopedRunnerWithReturn;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.PreparedStatementFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.listener.SqlListener;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.resource.ResourceAccessor;
import liquibase.resource.SearchPathResourceAccessor;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotControl;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.PrimaryKey;
import liquibase.structure.core.Sequence;
import liquibase.structure.core.Table;
import liquibase.ui.LoggerUIService;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.terminal.impl.DumbTerminal;
import org.jline.utils.InfoCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static copydb.StringUtil.parseBoolean;
import static copydb.StringUtil.parseInt;
import static copydb.StringUtil.trimToNull;
import static liquibase.util.StringUtil.processMultiLineSQL;

public class CopyDb {

    static final Logger LOG = LoggerFactory.getLogger(CopyDb.class.getSimpleName());
    private static final Logger SQL_LOG = LoggerFactory.getLogger("sql");

    private final JdbcProperties source;
    private final JdbcProperties target;
    private String changelog;
    private String resolvedChangelog;
    private ResourceAccessor resourceAccessor;
    private final ObjectFilter tableFilter = new ObjectFilter(true);
    private final ObjectFilter sequenceFilter = new ObjectFilter(false);
    private final ObjectFilter columnFilter = new ObjectFilter(true);
    private final ObjectFilter triggerFilter = new ObjectFilter(false);
    private final ObjectFilter disableTriggerFilter = new ObjectFilter(false);
    private Database targetDb;
    private Database sourceDb;
    private DatabaseSupport targetSupport;
    private DatabaseSupport sourceSupport;
    private DatabaseSnapshot sourceSnapshot;
    private DatabaseSnapshot targetSnapshot;
    private boolean dropFirst;
    private boolean truncate;
    private boolean disableForeignKeys = true;
    private boolean ddlOnly = false;
    private boolean orderByPrimaryKey;
    private boolean logSql = false;
    private int batchSize = 0;
    private String tag;
    private String contexts;
    private String labelFilter;
    private String classpath;
    private String searchPath;
    private String initSql;
    private String preCopySql;
    private String postSql;
    private List<Table> targetTables;
    private Map<CopyTypeKey, CopyTypeStat> typeStats = Map.of();
    private long totalRows;
    private long totalDatums;
    private String flashback;
    private final Terminal terminal;
    private final boolean plainOutput;

    public CopyDb(JdbcProperties source, JdbcProperties target) {
        this.source = source;
        this.target = target;
        Terminal term;
        try {
            term = TerminalBuilder.builder()
                .system(true)
                .build();
        } catch (IOException e) {
            try {
                term = TerminalBuilder.builder()
                    .dumb(true)
                    .build();
            } catch (IOException _ignored) {
                throw new IllegalStateException(e);
            }
        }
        this.terminal = term;
        this.plainOutput = term instanceof DumbTerminal;
    }

    public CopyDb() {
        this(new JdbcProperties(), new JdbcProperties());
        source.setReadonly(true);
    }

    public CopyDb(PropertySource config) {
        this();
        load(config);
    }

    /**
     * Load configuration from properties.
     */
    public void load(PropertySource config) {
        source.load(config, "source.");
        target.load(config, "target.");

        changelog = config.getProperty("changelog", changelog);
        tag = config.getProperty("tag", tag);
        contexts = config.getProperty("contexts", contexts);
        labelFilter = config.getProperty("label-filter", labelFilter);
        classpath = config.getProperty("classpath", classpath);
        searchPath = config.getProperty("search-path", searchPath);

        logSql = parseBoolean(config.getProperty("logging.sql"), logSql);
        batchSize = parseInt(config.getProperty("batch-size"), batchSize);
        truncate = parseBoolean(config.getProperty("truncate"), truncate);
        dropFirst = parseBoolean(config.getProperty("drop-first"), dropFirst);
        ddlOnly = parseBoolean(config.getProperty("ddl-only"), ddlOnly);
        disableForeignKeys = parseBoolean(config.getProperty("disable-foreign-keys"), disableForeignKeys);
        disableTriggerFilter.load(config, "disable-triggers.");
        tableFilter.load(config, "tables.");
        orderByPrimaryKey = config.getBoolean("tables.order-by-pk", false);
        sequenceFilter.load(config, "sequences.");
        columnFilter.load(config, "columns.");
        triggerFilter.load(config, "triggers.");

        initSql = config.getProperty("init-sql", initSql);
        preCopySql = config.getProperty("pre-copy-sql", preCopySql);
        postSql = config.getProperty("post-sql", postSql);
    }

    public JdbcProperties getSource() {
        return source;
    }

    public JdbcProperties getTarget() {
        return target;
    }

    public String getChangelog() {
        return changelog;
    }

    public void setChangelog(String changelog) {
        this.changelog = trimToNull(changelog);
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getContexts() {
        return contexts;
    }

    public void setContexts(String contexts) {
        this.contexts = contexts;
    }

    public String getLabelFilter() {
        return labelFilter;
    }

    public void setLabelFilter(String labelFilter) {
        this.labelFilter = labelFilter;
    }

    public String getClasspath() {
        return classpath;
    }

    public void setClasspath(String classpath) {
        this.classpath = classpath;
    }

    public String getSearchPath() {
        return searchPath;
    }

    public void setSearchPath(String searchPath) {
        this.searchPath = searchPath;
    }

    public ObjectFilter getTables() {
        return tableFilter;
    }

    public ObjectFilter getSequences() {
        return sequenceFilter;
    }

    public boolean isDdlOnly() {
        return ddlOnly;
    }

    public boolean isDropFirst() {
        return dropFirst;
    }

    public void setDropFirst(boolean dropFirst) {
        this.dropFirst = dropFirst;
    }

    public boolean isTruncate() {
        return truncate;
    }

    public void setTruncate(boolean truncate) {
        this.truncate = truncate;
    }

    public boolean isDisableForeignKeys() {
        return disableForeignKeys;
    }

    public void setDisableForeignKeys(boolean disableForeignKeys) {
        this.disableForeignKeys = disableForeignKeys;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public List<Table> getTargetTables() {
        return targetTables != null ? Collections.unmodifiableList(targetTables) : List.of();
    }

    public Map<CopyTypeKey, CopyTypeStat> getTypeStats() {
        return typeStats;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public long getTotalDatums() {
        return totalDatums;
    }

    public void copy() throws LiquibaseException, IOException {
        if (target.isReadonly()) {
            throw new IllegalStateException("Target database is read only: " + target.getUrl());
        }

        resolvedChangelog = this.changelog;
        if (searchPath == null) {
            if (changelog != null) {
                if (changelog.startsWith("classpath:")) {
                    resolvedChangelog = changelog.substring(10);
                    resourceAccessor = new ClassLoaderResourceAccessor();
                } else {
                    List<String> paths = new ArrayList<>();
                    var path = (changelog.startsWith("file:") ? Paths.get(URI.create(changelog)) : Paths.get(changelog)).toAbsolutePath();
                    var parent = path.getParent();
                    if (parent != null && parent.endsWith("db/changelog")
                        && Files.readString(path).matches("(?s).*(/?)db/changelog/.*")) {
                        resolvedChangelog = "db/changelog/" + path.getFileName().toString();
                        paths.add(parent.getParent().getParent().toString());
                    } else {
                        resolvedChangelog = path.getFileName().toString();
                        if (parent == null) {
                            parent = Paths.get("").toAbsolutePath();
                        }
                        paths.add(parent.toString());
                    }
                    paths.add(".");
                    resourceAccessor = new SearchPathResourceAccessor(String.join(",", paths));
                }
            } else {
                resourceAccessor = new DirectoryResourceAccessor(Paths.get(""));
            }
        } else {
            resourceAccessor = new SearchPathResourceAccessor(searchPath);
        }

        try {
            Map<String, Object> attrs = Map.of(
                Scope.Attr.ui.name(), new LoggerUIService());
            Scope.child(sqlLogger(), attrs, this::doCopy);
        } catch (LiquibaseException | RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw new LiquibaseException(e);
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    private Database openDatabase(JdbcProperties properties) throws DatabaseException {
        var dbFactory = DatabaseFactory.getInstance();
        var conn = dbFactory.openConnection(properties.getUrl(),
            properties.getUsername(),
            properties.getPassword(),
            null, resourceAccessor);
        var db = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(conn);
        if (properties.getSchema() != null) {
            db.setDefaultSchemaName(properties.getSchema());
        }
        if (properties.getLiquibaseSchema() != null) {
            db.setLiquibaseSchemaName(properties.getLiquibaseSchema());
        } else if (properties.getSchema() != null) {
            db.setLiquibaseSchemaName(properties.getSchema());
        }
        return db;
    }

    private void doCopy() throws Exception {
        try {
            sourceDb = openDatabase(source);
            sourceSupport = Databases.getDatabase(sourceDb);
            targetDb = openDatabase(target);
            targetSupport = Databases.getDatabase(targetDb);

            if (triggerFilter.isEnabled() && !targetSupport.canCopyTriggers(sourceDb, targetDb)) {
                throw new IllegalArgumentException("Cannot copy triggers from " + sourceDb.getShortName() + " to " + targetDb.getShortName());
            }

            if (batchSize <= 0) {
                batchSize = targetSupport.getDefaultBatchSize();
            }

            targetDb.setAutoCommit(false);

            DatabaseSupport.Action enableTriggers = DatabaseSupport.Action.NOP;
            try {
                runSql(sourceDb, source.getInitSql());
                flashback = "";
                if ("oracle".equals(sourceDb.getShortName())) {
                    try {
                        sourceSupport.querySingle(sourceDb, "Getting system change number", "SELECT current_scn FROM v$database", BigInteger.class)
                            .ifPresent(scn -> flashback = " AS OF SCN " + scn);
                    } catch (LiquibaseException e) {
                        // IGNORED
                    }
                }

                runSql(targetDb, target.getInitSql());

                runSql(targetDb, initSql);


                if (dropFirst) {
                    targetSupport.dropAll(targetDb);
                }

                targetSupport.createDefaultSchemas(targetDb);

                if (resolvedChangelog != null) {
                    runChangelog(targetDb);
                }

                runSql(targetDb, preCopySql);

                sourceSnapshot = takeSnapshot(sourceDb, false);
                targetSnapshot = takeSnapshot(targetDb, true);

                if (triggerFilter.isEnabled()) {
                    copyTriggers();
                }

                if (!disableTriggerFilter.excludesEverything()) {
                    enableTriggers = targetSupport.disableTriggers(targetDb, disableTriggerFilter);
                }

                if (sequenceFilter.isEnabled()) {
                    copySequences();
                }

                if (!ddlOnly && tableFilter.isEnabled()) {
                    copyTables();
                }

                if (targetSupport.getDdlCommitMode() == DatabaseSupport.DdlCommitMode.TRANSACTIONAL) {
                    enableTriggers.execute();
                    targetDb.commit();
                } else {
                    targetDb.commit();
                    enableTriggers.execute();
                }

            } catch (LiquibaseException e) {
                try {
                    targetDb.rollback();
                } catch (Exception nested) {
                    e.addSuppressed(nested);
                }

                if (targetSupport.getDdlCommitMode() != DatabaseSupport.DdlCommitMode.TRANSACTIONAL) {
                    try {
                        enableTriggers.execute();
                    } catch (Exception nested) {
                        e.addSuppressed(nested);
                    }
                }

                throw e;
            }

            runSql(targetDb, postSql);
        } finally {
            if (sourceDb != null) {
                try {
                    sourceDb.close();
                } catch (DatabaseException e) {
                    LOG.warn("Error closing source data base", e);
                }
            }
            if (targetDb != null) {
                try {
                    targetDb.close();
                } catch (DatabaseException e) {
                    LOG.warn("Error closing target data base", e);
                }
            }
            sourceDb = null;
            sourceSnapshot = null;
            targetDb = null;
            targetSupport = null;
            targetSnapshot = null;
        }
    }

    static class SqlOptions {
        boolean split = true;
        boolean comments = true;
        String delimiter = null;
        boolean ignoreErrors = false;
        // > 0: commit every N statements, 0: commit at end, -1: don't commit explicitly
        int commit = 0;

        boolean parse(String params) {
            boolean any = false;
            String error = null;
            for (var p : params.split("[ ,]+")) {
                var s = p.split("=", 2);
                var name = s[0];
                if (name.isEmpty()) {
                    continue;
                }
                var val = s.length == 2 ? s[1] : "";
                if ("delimiter".equalsIgnoreCase(name)) {
                    delimiter = val;
                } else if ("split".equalsIgnoreCase(name)) {
                    split = StringUtil.parseBoolean(val, true);
                } else if ("nosplit".equalsIgnoreCase(name)) {
                    split = !StringUtil.parseBoolean(val, true);
                } else if ("comments".equalsIgnoreCase(name)) {
                    comments = StringUtil.parseBoolean(val, true);
                } else if ("nocomments".equalsIgnoreCase(name)) {
                    comments = !StringUtil.parseBoolean(val, true);
                } else if ("commit".equalsIgnoreCase(name)) {
                    commit = parseBoolean(val)
                        .map(t -> t ? 1 : 0)
                        .orElseGet(() -> {
                            final int i;
                            try {
                                i = Integer.parseInt(val);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(name + ": illegal value: " + val);
                            }
                            if (i < -1) {
                                throw new IllegalArgumentException(name + ": illegal value: " + val);
                            }
                            return i;
                        });
                } else if ("errors".equalsIgnoreCase(name)) {
                    if ("ignore".equalsIgnoreCase(val) || "skip".equalsIgnoreCase(val)) {
                        ignoreErrors = true;
                    } else if ("fail".equalsIgnoreCase(val) || "stop".equalsIgnoreCase(val)) {
                        ignoreErrors = false;
                    } else {
                        throw new IllegalArgumentException("Invalid option: " + p);
                    }
                } else {
                    if (error == null) {
                        error = name;
                    }
                    continue;
                }

                any = true;
            }

            if (error != null && any) {
                throw new IllegalArgumentException("Invalid option: " + error);
            }

            return any;
        }
    }

    private void runSql(Database db, String sqlParam) throws LiquibaseException {
        if (StringUtil.isEmpty(sqlParam)) {
            return;
        }

        SqlOptions options = new SqlOptions();
        Matcher m = Pattern.compile("(.+); ?--[ \t]*(.+)$").matcher(sqlParam);
        if (m.find()) {
            String params = m.group(2);
            if (options.parse(params)) {
                sqlParam = m.group(1);
            }
        }

        String sql;
        try {
            if (sqlParam.startsWith("file:")) {
                sql = String.join("\n", Files.readAllLines(Paths.get(URI.create(sqlParam))));
            } else if (sqlParam.startsWith("@")) {
                sql = String.join("\n", Files.readAllLines(Paths.get(sqlParam.substring(1))));
            } else {
                sql = sqlParam;
            }
        } catch (IOException e) {
            throw new LiquibaseException(e);
        }

        if (sql.startsWith("--") && !sqlParam.equals(sql)) {
            int end = sql.indexOf('\n');
            if (end != -1 && end != sql.length() - 1) {
                if (options.parse(sql.substring(2, end))) {
                    sql = sql.substring(end + 1);
                }
            }
        }

        SqlStatement[] statements;
        if (sql.indexOf('\n') != -1) {
            statements = Arrays.stream(processMultiLineSQL(sql, !options.comments, options.split, options.delimiter))
                .map(s -> new RawParameterizedSqlStatement(s, options.delimiter))
                .toArray(SqlStatement[]::new);
        } else {
            statements = new SqlStatement[]{new RawParameterizedSqlStatement(sql, options.delimiter)};
        }

        if (options.ignoreErrors || options.commit > 0) {
            int count = 0;
            for (var stmt : statements) {
                try {
                    db.execute(new SqlStatement[]{stmt}, List.of());
                    count++;
                    if (count == options.commit) {
                        db.commit();
                        count = 0;
                    }
                } catch (DatabaseException e) {
                    try {
                        db.rollback();
                    } catch (Exception nested) {
                        e.addSuppressed(nested);
                    }
                    if (options.ignoreErrors) {
                        Throwable cause = e.getCause() instanceof SQLException ? e.getCause() : e;
                        LOG.error("Ingoring SQL error", cause);
                    } else {
                        throw e;
                    }
                }
            }
        } else {
            try {
                db.execute(statements, List.of());
            } catch (Exception e) {
                try {
                    db.rollback();
                } catch (Exception nested) {
                    e.addSuppressed(nested);
                }
                throw e;
            }
        }

        if (options.commit == 0) {
            try {
                db.commit();
            } catch (DatabaseException e) {
                try {
                    db.rollback();
                } catch (Exception nested) {
                    e.addSuppressed(nested);
                }
                if (options.ignoreErrors) {
                    Throwable cause = e.getCause() instanceof SQLException ? e.getCause() : e;
                    LOG.error("Ingoring commit error", cause);
                } else {
                    throw e;
                }
            }
        }
    }

    private SqlListener sqlLogger() {
        return new SqlListener() {
            @Override
            public void readSqlWillRun(String sql) {
                writeSqlWillRun(sql);
            }

            @Override
            public void writeSqlWillRun(String sql) {
                if (logSql) {
                    SQL_LOG.info("{}", sql);
                }
            }
        };
    }

    private DatabaseSnapshot takeSnapshot(Database db, boolean foreignKeys) throws LiquibaseException {
        CatalogAndSchema[] schemas = {db.getDefaultSchema()};
        List<Class<? extends DatabaseObject>> types = new ArrayList<>(4);
        if (sequenceFilter.isEnabled()) {
            types.add(Sequence.class);
        }
        if (tableFilter.isEnabled()) {
            types.addAll(Arrays.asList(Table.class, Column.class));
            if (foreignKeys) {
                types.add(ForeignKey.class);
            }
            if (orderByPrimaryKey && db == sourceDb) {
                types.add(PrimaryKey.class);
            }
        }
        var ctl = new SnapshotControl(db);
        ctl.getTypesToInclude().clear();
        if (!types.isEmpty()) {
            types.forEach(t -> ctl.addType(t, db));
        }
        return withoutSqlLogging(() -> SnapshotGeneratorFactory.getInstance().createSnapshot(schemas, db, ctl));
    }


    private <T> T withSqlLogging(boolean logSql, ScopedRunnerWithReturn<T> runner) throws LiquibaseException {
        boolean prev = this.logSql;
        this.logSql = logSql;
        try {
            return runner.run();
        } catch (LiquibaseException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new LiquibaseException(e);
        } finally {
            this.logSql = prev;
        }
    }

    private <T> T withoutSqlLogging(ScopedRunnerWithReturn<T> runner) throws LiquibaseException {
        return withSqlLogging(false, runner);
    }

    private void copySequences() throws LiquibaseException {
        var orderedSequences = sourceSnapshot.get(Sequence.class).stream()
                .filter(s -> sequenceFilter.contains(s.getName()))
                .collect(Collectors.toCollection(ArrayList::new));
        sequenceFilter.sort(orderedSequences, DatabaseObject::getName);
        for (var seq : orderedSequences) {
            var targetSeq = targetSnapshot.get(Sequence.class).stream()
                    .filter(t -> t.getName().equalsIgnoreCase(seq.getName()))
                    .findFirst()
                    .orElse(null);
            targetSupport.copySequence(seq, targetDb, targetSeq);
        }
    }

    private void copyTriggers() throws LiquibaseException {
        var support = Databases.getDatabase(sourceDb);
        var triggers = support.getTriggers(sourceDb)
            .stream()
            .filter(t -> triggerFilter.contains(t.getTable() != null ? t.getTable().getName() : null, t.getName()))
            .toList();

        targetSupport.createTriggers(targetDb, triggers);
    }

    private void copyTables() throws LiquibaseException {
        if (tableFilter.excludesEverything()) {
            return;
        }

        var sourceTables = sourceSnapshot.get(Table.class).stream()
            .filter(t -> tableFilter.contains(t.getName()))
            .collect(Collectors.toMap(e -> e.getName().toLowerCase(Locale.ROOT), e -> e));
        if (sourceTables.isEmpty()) {
            if (tableFilter.isEmpty()) {
                LOG.error("No source tables");
            } else {
                LOG.error("No source tables matching filter");
            }
            return;
        }

        typeStats = new TreeMap<>();
        targetTables = targetSnapshot.get(Table.class).stream()
            .filter(t -> tableFilter.contains(t.getName()))
            .filter(t -> sourceTables.containsKey(t.getName().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toCollection(ArrayList::new));

        if (targetTables.isEmpty()) {
            LOG.error("No target tables. Check liquibase changelog or create tables first.");
            return;
        }

        tableFilter.sort(targetTables, Table::getName);

        var enableForeignKeys = disableForeignKeys();
        try {
            if (truncate) {
                for (var targetTable : targetTables) {
                    targetSupport.truncateTable(targetDb, targetTable);
                    if (targetSupport.getDataCommitMode() != DatabaseSupport.DataCommitMode.ONCE) {
                        targetDb.commit();
                    }
                }
            }

            for (var targetTable : targetTables) {
                var sourceTable = sourceTables.get(targetTable.getName().toLowerCase(Locale.ROOT));
                copyTable(sourceTable, targetTable);
            }
        } catch (Exception e) {
            LOG.error("Error copying tables: {}", e.toString());
            if (targetSupport.getDdlCommitMode() != DatabaseSupport.DdlCommitMode.TRANSACTIONAL) {
                try {
                    targetDb.rollback();
                } catch (Exception nested) {
                    e.addSuppressed(nested);
                }

                try {
                    enableForeignKeys.execute();
                } catch (Exception nested) {
                    e.addSuppressed(nested);
                }
            }

            throw e;
        }

        enableForeignKeys.execute();
    }

    private static boolean isVirtualColumn(Column c) {
        return c.getDefaultValue() instanceof DatabaseFunction func
            && func.getValue() != null
            && func.getValue().toUpperCase(Locale.ROOT).startsWith("GENERATED ALWAYS AS ");
    }

    private List<Column> filterTargetColumns(Database database, Table table, Table sourceTable) {
        var columns = table.getColumns().stream()
            .filter(c -> sourceTable.getColumn(c.getName()) != null
                && !isVirtualColumn(c))
            .toList();

        if (columnFilter.isEnabled() && !(columnFilter.getInclude().isEmpty() && columnFilter.getExclude().isEmpty())) {
            columns = columns.stream()
                .filter(c -> columnFilter.contains(table.getName(), c.getName()))
                .toList();
        }

        return columns;
    }

    private void copyTable(Table sourceTable, Table targetTable) throws LiquibaseException {
        PreparedStatementFactory sourceStmtFactory = new PreparedStatementFactory((JdbcConnection) sourceDb.getConnection());
        PreparedStatementFactory targetStmtFactory = new PreparedStatementFactory((JdbcConnection) targetDb.getConnection());

        var columns = filterTargetColumns(targetDb, targetTable, sourceTable);
        final var insertSql = insertSqlForTable(targetTable, columns);
        var converters = new ArrayList<ColumnDescriptor<?, ?>>(columns.size());
        StringBuilder sb = new StringBuilder()
            .append("TABLE ")
            .append(targetDb.escapeTableName(null, null, targetTable.getName()));
        for (var targetColumn : columns) {
            var sourceColumn = sourceTable.getColumn(targetColumn.getName());
            var converter = Converters.converterFor(sourceDb, sourceColumn, targetDb, targetColumn);
            sb.append('\n').append("  ").append(converter);
            converters.add(converter);
        }
        LOG.debug(sb.toString());

        String qualifiedSourceTable = sourceDb.escapeTableName(null, null, sourceTable.getName());
        String selectSql = "SELECT * FROM " + qualifiedSourceTable + flashback;
        if (orderByPrimaryKey) {
            var pk = sourceTable.getPrimaryKey();
            if (pk != null) {
                selectSql += " ORDER BY " + String.join(", ", pk.getColumnNamesAsList());
            }
        }

        if (logSql) {
            SQL_LOG.info(selectSql);
            SQL_LOG.info(insertSql.replaceFirst(" VALUES\\(.*$", ""));
        }

        try (var count = sourceStmtFactory.create("SELECT COUNT(*) FROM " + qualifiedSourceTable + flashback);
             var select = sourceStmtFactory.create(selectSql);
             var insert = targetStmtFactory.create(insertSql)) {
            long total;
            try (var rs = count.executeQuery()) {
                rs.next();
                total = rs.getLong(1);
                totalRows += total;
                totalDatums += total * converters.size();
                for (var i = 0; i < converters.size(); i++) {
                    var converter = converters.get(i);
                    var stat = typeStats.computeIfAbsent(new CopyTypeKey(converter.getSource().getType(), converter.getTarget().getType()),
                        CopyTypeStat::new);
                    stat.incrementOccurs();
                    stat.incrementRows(total);
                }
            }

            terminal.puts(InfoCmp.Capability.cursor_invisible);
            var progress = new CopyTableProgress(targetTable.getName(), total);
            if (!plainOutput) {
                progress.print(terminal);
            }
            var rs = select.executeQuery();
            int rowsInBatch = 0;
            while (rs.next()) {
                for (var i = 0; i < converters.size(); i++) {
                    var conv = converters.get(i);
                    conv.copy(rs, insert, i + 1);
                }

                progress.increment();
                insert.addBatch();
                if (++rowsInBatch == batchSize) {
                    rowsInBatch = 0;
                    insert.executeBatch();
                    if (targetSupport.getDataCommitMode() == DatabaseSupport.DataCommitMode.BATCH) {
                        targetDb.commit();
                    }
                    if (progress.percent() >= 100) {
                        progress.stop();
                    }
                    progress.print(terminal);
                } else if (!plainOutput && progress.shouldUpdate()) {
                    progress.print(terminal);
                }
            }

            if (rowsInBatch != 0) {
                insert.executeBatch();
                if (targetSupport.getDataCommitMode() == DatabaseSupport.DataCommitMode.BATCH) {
                    targetDb.commit();
                }
            }

             if (total == 0 || rowsInBatch != 0) {
                 progress.stop();
                 progress.print(terminal);
             }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } finally {
            if (!plainOutput) {
                terminal.puts(InfoCmp.Capability.cursor_visible);
                terminal.writer().println();
            }
            terminal.flush();
        }
    }

    private String insertSqlForTable(Table table, List<Column> columns) {
        String qualifiedName = targetDb.escapeTableName(null, null, table.getName());
        var sql = new StringBuilder();
        sql.append("INSERT INTO ");
        sql.append(qualifiedName);
        sql.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sql.append(',');
            }
            var col = columns.get(i);
            String colName = targetDb.escapeColumnName(null, null, table.getName(), col.getName());
            sql.append(colName);
        }
        sql.append(") VALUES(");
        for (int i = 0; i < columns.size(); i++) {
            sql.append(i == 0 ? "?" : ",?");
        }
        sql.append(")");
        return sql.toString();
    }

    private DatabaseSupport.Action disableForeignKeys() throws LiquibaseException {
        if (!disableForeignKeys) {
            return DatabaseSupport.Action.NOP;
        }

        var keys = targetSnapshot.get(ForeignKey.class).stream()
            .filter(k -> tableFilter.contains(k.getForeignKeyTable().getName()))
            .sorted(Comparator.comparing(ForeignKey::getName))
            .toList();
        if (keys.isEmpty()) {
            return DatabaseSupport.Action.NOP;
        }

        return targetSupport.disableForeignKeys(targetDb, keys);
    }

    private ClassLoader createClassLoader(ClassLoader parent) {
        if (classpath == null) {
            return parent;
        }

        final List<URL> urls = new ArrayList<>();
        for (String classpathEntry : classpath.split(File.pathSeparator)) {
            File f = new File(classpathEntry);
            if (f.exists()) {
                try {
                    URL url = new File(classpathEntry).toURI().toURL();
                    urls.add(url);
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        }

        if (urls.isEmpty()) {
            return parent;
        } else {
            return new URLClassLoader(urls.toArray(URL[]::new), parent);
        }
    }

    private void runChangelog(Database targetDb) throws Exception {
        var classLoader = createClassLoader(Thread.currentThread().getContextClassLoader());
        Scope.child(Scope.Attr.classLoader, classLoader, () -> {
            @SuppressWarnings("resource")
            var liquibase = new Liquibase(resolvedChangelog, resourceAccessor, targetDb);
            liquibase.update(tag, new Contexts(contexts), new LabelExpression(labelFilter));
            tableFilter.getExclude().addAll(Arrays.asList("DATABASECHANGELOG", "DATABASECHANGELOGLOCK"));
        });
    }

    public static class CopyTypeKey implements Comparable<CopyTypeKey> {
        private final DataType from;
        private final DataType to;
        private final String key;
        private final int hashCode;

        public CopyTypeKey(DataType from, DataType to) {
            this.from = from;
            this.to = to;
            if (from.getColumnSize() != null && to.getColumnSize() != null) {
                String fromStr = from.toString().replaceFirst(" (CHAR|BYTE)\\)$", ")");
                String toStr = to.toString().replaceFirst(" (CHAR|BYTE)\\)$", ")");;
                if (fromStr.equalsIgnoreCase(toStr)) {
                    this.key = from.getTypeName() + "(*) -> " + to.getTypeName() + "(*)";
                } else {
                    this.key = sanitizeDataType(fromStr) + " -> " + sanitizeDataType(toStr);
                }
            } else {
                this.key = sanitizeDataType(from) + " -> " + sanitizeDataType(to);
            }
            this.hashCode = this.key.hashCode();
        }

        private static String sanitizeDataType(DataType t) {
            String s = t.toString();
            if (t.getColumnSize() == null) {
                return s;
            }

            return sanitizeDataType(s);
        }

        private static String sanitizeDataType(String s) {
            // TIMESTAMP(n)(m) -> TIMESTAMP(n)
            return s.replaceFirst("\\)\\(\\d+\\)$", ")");
        }

        public DataType getFrom() {
            return from;
        }

        public DataType getTo() {
            return to;
        }

        @Override
        public int compareTo(CopyTypeKey o) {
            return key.compareTo(o.key);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            CopyTypeKey that = (CopyTypeKey) o;
            return Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            return key;
        }
    }

    public static class CopyTypeStat {
        private final CopyTypeKey key;
        private long occurs;
        private long rows;

        public CopyTypeStat(CopyTypeKey key) {
            this.key = key;
        }

        public CopyTypeKey getKey() {
            return key;
        }

        public long getOccurs() {
            return occurs;
        }

        public long getRows() {
            return rows;
        }

        public void incrementOccurs() {
            occurs += 1;
        }

        public void incrementRows(long n) {
            rows += n;
        }

        @Override
        public String toString() {
            return key + ": " + occurs + " occurrences, " + rows + " rows";
        }

    }

}
