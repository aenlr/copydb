package copydb;

import copydb.convert.ColumnDescriptor;
import copydb.convert.Converters;
import copydb.db.DatabaseSupport;
import copydb.db.Databases;
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
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.PrimaryKey;
import liquibase.structure.core.Sequence;
import liquibase.structure.core.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
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
    private DatabaseSnapshot sourceSnapshot;
    private DatabaseSnapshot targetSnapshot;
    private boolean dropFirst;
    private boolean truncate;
    private boolean disableForeignKeys = true;
    private boolean orderByPrimaryKey;
    private boolean logSql = true;
    private int batchSize = 0;
    private String tag;
    private String contexts;
    private String labelFilter;
    private String classpath;
    private String searchPath;
    private String initSql;
    private String preCopySql;
    private String postSql;

    public CopyDb(JdbcProperties source, JdbcProperties target) {
        this.source = source;
        this.target = target;
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
            Scope.child(sqlLogger(), this::doCopy);
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
        var conn = dbFactory.openConnection(properties.getUrl(), properties.getUsername(), properties.getPassword(), null, resourceAccessor);
        return DatabaseFactory.getInstance().findCorrectDatabaseImplementation(conn);
    }

    private void doCopy() throws Exception {
        try {
            sourceDb = openDatabase(source);
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
                runSql(targetDb, target.getInitSql());

                runSql(targetDb, initSql);


                if (dropFirst) {
                    targetSupport.dropAll(targetDb);
                }

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

                if (tableFilter.isEnabled()) {
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

        List<Table> targetTables = targetSnapshot.get(Table.class).stream()
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

    private static boolean isOracleVirtualColumn(Column c) {
        if (c.getComputed() != null) {
            return false;
        }

        return c.getDefaultValue() instanceof DatabaseFunction func
            && func.getValue() != null
            && func.getValue().startsWith("GENERATED ALWAYS AS ");
    }

    private List<Column> filterTargetColumns(Database database, Table table, Table sourceTable) {
        var columns = table.getColumns().stream()
            .filter(c -> sourceTable.getColumn(c.getName()) != null)
            .toList();

        if ("oracle".equals(database.getShortName())) {
            // Filter out virtual columns
            columns = columns.stream()
                .filter(c -> !isOracleVirtualColumn(c))
                .toList();
        }

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
        StringBuilder sb = new StringBuilder();
        sb.append("TABLE ").append(targetTable.getName()).append('\n');
        for (var targetColumn : columns) {
            var sourceColumn = sourceTable.getColumn(targetColumn.getName());
            var converter = Converters.converterFor(sourceDb, sourceColumn, targetDb, targetColumn);
            sb.append("  ").append(converter).append('\n');
            converters.add(converter);
        }
        LOG.debug(sb.toString());

        String selectSql = "SELECT * FROM " + sourceTable.getName();
        if (orderByPrimaryKey) {
            var pk = sourceTable.getPrimaryKey();
            if (pk != null) {
                selectSql += " ORDER BY " + String.join(", ", pk.getColumnNamesAsList());
            }
        }

        if (logSql) {
            SQL_LOG.info(selectSql);
            SQL_LOG.info(insertSql);
        }

        try (var count = sourceStmtFactory.create("SELECT COUNT(*) FROM " + sourceTable.getName());
             var select = sourceStmtFactory.create(selectSql);
             var insert = targetStmtFactory.create(insertSql)) {
            long total;
            try (var rs = count.executeQuery()) {
                rs.next();
                total = rs.getLong(1);
            }

            var rs = select.executeQuery();
            int rowsInBatch = 0;
            long row = 0;
            while (rs.next()) {
                for (var i = 0; i < converters.size(); i++) {
                    var conv = converters.get(i);
                    conv.copy(rs, insert, i + 1);
                }

                row++;
                insert.addBatch();
                if (++rowsInBatch == batchSize) {
                    rowsInBatch = 0;
                    LOG.info("Loading {} {}/{} rows ({}%)", targetTable.getName(), row, total, 100 * row / total);
                    insert.executeBatch();
                    if (targetSupport.getDataCommitMode() == DatabaseSupport.DataCommitMode.BATCH) {
                        targetDb.commit();
                    }
                }
            }

            if (rowsInBatch != 0) {
                LOG.info("Loading {} {}/{} rows ({}%)", targetTable.getName(), row, total, 100 * row / total);
                insert.executeBatch();
                if (targetSupport.getDataCommitMode() == DatabaseSupport.DataCommitMode.BATCH) {
                    targetDb.commit();
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private String insertSqlForTable(Table table, List<Column> columns) {
        var sql = new StringBuilder();
        sql.append("INSERT INTO ");
        sql.append(table.getName());
        sql.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sql.append(',');
            }
            sql.append(columns.get(i).getName());
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

}
