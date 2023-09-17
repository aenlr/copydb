package copydb;

import liquibase.CatalogAndSchema;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.Scope;
import liquibase.Scope.ScopedRunner;
import liquibase.Scope.ScopedRunnerWithReturn;
import liquibase.change.core.AddUniqueConstraintChange;
import liquibase.change.core.CreateIndexChange;
import liquibase.change.core.CreateSequenceChange;
import liquibase.change.core.CreateTableChange;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.command.CommandScope;
import liquibase.command.core.GenerateChangelogCommandStep;
import liquibase.command.core.helpers.DbUrlConnectionCommandStep;
import liquibase.command.core.helpers.PreCompareCommandStep;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.DatabaseFactory;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.database.PreparedStatementFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.CompareControl.SchemaComparison;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.listener.SqlListener;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.resource.ResourceAccessor;
import liquibase.resource.SearchPathResourceAccessor;
import liquibase.serializer.core.xml.XMLChangeLogSerializer;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotControl;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.CreateSequenceStatement;
import liquibase.statement.core.DropSequenceStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Column;
import liquibase.structure.core.DatabaseObjectFactory;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Sequence;
import liquibase.structure.core.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static copydb.StringUtil.*;
import static liquibase.util.StringUtil.processMultiLineSQL;

public class CopyDb {

    static final Logger LOG = LoggerFactory.getLogger(CopyDb.class.getSimpleName());
    private static final Logger SQL_LOG = LoggerFactory.getLogger("sql");

    private final JdbcProperties source;
    private final JdbcProperties target;
    private String changelog;
    private String resolvedChangelog;
    private ResourceAccessor resourceAccessor;
    private final ObjectFilter tables = new ObjectFilter(true);
    private final ObjectFilter sequences = new ObjectFilter(false);
    private boolean dropFirst;
    private boolean truncate;
    private boolean disableForeignKeys = true;
    private boolean disableTriggers = true;
    private boolean logSql = true;
    private int batchSize = 500;
    private String tag;
    private String contexts;
    private String labelFilter;
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

    public void load(PropertySource config) {
        source.load(config, "source.");
        target.load(config, "target.");

        changelog = config.getProperty("changelog", changelog);
        tag = config.getProperty("tag", tag);
        contexts = config.getProperty("contexts", contexts);
        labelFilter = config.getProperty("label-filter", labelFilter);
        searchPath = config.getProperty("search-path", searchPath);

        logSql = parseBoolean(config.getProperty("logging.sql"), logSql);
        batchSize = parseInt(config.getProperty("batch-size"), batchSize);
        truncate = parseBoolean(config.getProperty("truncate"), truncate);
        dropFirst = parseBoolean(config.getProperty("drop-first"), dropFirst);
        disableForeignKeys = parseBoolean(config.getProperty("disable-foreign-keys"), disableForeignKeys);
        disableTriggers = parseBoolean(config.getProperty("disable-triggers"), disableTriggers);
        tables.load(config, "tables.");
        sequences.load(config, "sequences.");

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

    public String getSearchPath() {
        return searchPath;
    }

    public void setSearchPath(String searchPath) {
        this.searchPath = searchPath;
    }

    public ObjectFilter getTables() {
        return tables;
    }

    public ObjectFilter getSequences() {
        return sequences;
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
                    if ("auto".equals(changelog)) {
                        resolvedChangelog = null;
                    } else {
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

    private void doCopy() throws Exception {
        try (var sourceConn = getDatabaseConnection(source);
             var targetConn = getDatabaseConnection(target)) {
            var sourceDb = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(sourceConn);
            var targetDb = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(targetConn);

            targetDb.setAutoCommit(false);

            runSql(sourceDb, source.getInitSql());
            runSql(targetDb, target.getInitSql());

            if ("auto".equals(changelog)) {
                generateChangeLog(sourceDb, targetDb);
            }

            runSql(targetDb, initSql);

            if (dropFirst) {
                // TODO: drop triggers
                var liquibase = new Liquibase((DatabaseChangeLog)null, resourceAccessor, targetDb);
                liquibase.dropAll();
            }

            if (resolvedChangelog != null) {
                runChangelog(targetDb);
            }

            runSql(targetDb, preCopySql);

            var sourceSnapshot = takeSnapshot(sourceDb, false);
            var targetSnapshot = takeSnapshot(targetDb, true);
            if (sequences.isEnabled()) {
                copySequences(sourceSnapshot, targetSnapshot, targetDb);
            }
            if (tables.isEnabled()) {
                copyTables(sourceSnapshot, targetSnapshot);
            }

            runSql(targetDb, postSql);
        }
    }

    static class SqlOptions {
        boolean split = true;
        boolean comments = true;
        String delimiter = null;
        boolean ignoreErrors = false;

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
                .map(s -> new RawSqlStatement(s, options.delimiter))
                .toArray(SqlStatement[]::new);
        } else {
            statements = new SqlStatement[]{ new RawSqlStatement(sql, options.delimiter) };
        }

        if (options.ignoreErrors) {
            for (var stmt : statements) {
                try {
                    db.execute(new SqlStatement[]{ stmt }, List.of());
                    db.commit();
                } catch (DatabaseException e) {
                    db.rollback();
                    Throwable cause = e.getCause() instanceof SQLException ? e.getCause() : e;
                    LOG.error("Ingoring SQL error", cause);
                }
            }
        } else {
            try {
                db.execute(statements, List.of());
                db.commit();
            } catch (Exception e) {
                db.rollback();
                throw e;
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
        if (sequences.isEnabled()) {
            types.add(Sequence.class);
        }
        if (tables.isEnabled()) {
            types.addAll(Arrays.asList(Table.class, Column.class));
            if (foreignKeys) {
                types.add(ForeignKey.class);
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

    private void withSqlLogging(boolean logSql, ScopedRunner<?> runner) throws LiquibaseException {
        withSqlLogging(logSql, () -> {
            runner.run();
            return null;
        });
    }

    private <T> T withoutSqlLogging(ScopedRunnerWithReturn<T> runner) throws LiquibaseException {
        return withSqlLogging(false, runner);
    }

    private void withoutSqlLogging(ScopedRunner<?> runner) throws LiquibaseException {
        withSqlLogging(false, runner);
    }

    private static final BigInteger BIGINT_64_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    private static final Map<String, BigInteger> DB_TYPE_SEQUENCE_MAX_MAX = Map.of(
        "oracle", new BigInteger("9999999999999999999999999999"), // 28 digits
        "postgresql", BIGINT_64_MAX,
        "mssql", BIGINT_64_MAX,
        "h2", BIGINT_64_MAX // Does not complain if value is out of range (for Oracle compatibility?)
    );

    private void copySequences(DatabaseSnapshot sourceSnapshot, DatabaseSnapshot targetSnapshot, Database target) throws LiquibaseException {
        var orderedSequences = sourceSnapshot.get(Sequence.class).stream()
            .filter(s -> sequences.contains(s.getName()))
            .collect(Collectors.toCollection(ArrayList::new));
        sequences.sort(orderedSequences, DatabaseObject::getName);
        var sourceSequenceMaxMax = DB_TYPE_SEQUENCE_MAX_MAX.get(sourceSnapshot.getDatabase().getShortName());
        var targetSequenceMaxMax = DB_TYPE_SEQUENCE_MAX_MAX.get(targetSnapshot.getDatabase().getShortName());
        for (var seq : orderedSequences) {
            var targetSeq = targetSnapshot.get(Sequence.class).stream()
                .filter(t -> t.getName().equalsIgnoreCase(seq.getName()))
                .findFirst()
                .orElse(null);

            var startValue = seq.getStartValue();
            if (targetSeq == null || !startValue.equals(targetSeq.getStartValue())) {
                List<SqlStatement> stmts = new ArrayList<>(2);
                if (targetSeq != null) {
                    stmts.add(new DropSequenceStatement(target.getDefaultCatalogName(), target.getDefaultSchemaName(), seq.getName()));
                }

                var create = new CreateSequenceStatement(target.getDefaultCatalogName(), target.getDefaultSchemaName(), seq.getName());
                create.setCacheSize(seq.getCacheSize());
                create.setIncrementBy(seq.getIncrementBy());
                create.setMinValue(seq.getMinValue());
                if (seq.getMaxValue() != null) {
                    if (targetSequenceMaxMax != null && seq.getMaxValue().compareTo(targetSequenceMaxMax) > 0) {
                        create.setMaxValue(targetSequenceMaxMax);
                    } else if (sourceSequenceMaxMax == null || !seq.getMaxValue().equals(sourceSequenceMaxMax)) {
                        create.setMaxValue(seq.getMaxValue());
                    }
                }
                create.setOrdered(seq.getOrdered());
                create.setCycle(seq.getWillCycle());
                create.setStartValue(startValue);
                stmts.add(create);
                target.execute(stmts.toArray(SqlStatement[]::new), List.of());
            }
        }
    }

    private void copyTables(DatabaseSnapshot sourceSnapshot,
                            DatabaseSnapshot targetSnapshot) throws LiquibaseException {
        if (tables.getInclude().isEmpty() && tables.getExclude().contains("*")) {
            return;
        }

        var sourceTables = sourceSnapshot.get(Table.class).stream()
            .filter(t -> tables.contains(t.getName()))
            .collect(Collectors.toMap(e -> e.getName().toLowerCase(Locale.ROOT), e -> e));

        List<Table> targetTables = targetSnapshot.get(Table.class).stream()
            .filter(t -> tables.contains(t.getName()))
            .filter(t -> sourceTables.containsKey(t.getName().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toCollection(ArrayList::new));

        if (targetTables.isEmpty()) {
            return;
        }

        tables.sort(targetTables, Table::getName);

        if (disableTriggers) {
            toggleTriggers(targetSnapshot, false);
        }

        if (disableForeignKeys) {
            toggleForeignKeys(targetSnapshot, false);
        }

        try {
            var sourceDb = sourceSnapshot.getDatabase();
            var targetDb = targetSnapshot.getDatabase();
            if (truncate) {
                for (var targetTable : targetTables) {
                    truncateTable(targetDb, targetTable);
                }
            }

            for (var targetTable : targetTables) {
                var sourceTable = sourceTables.get(targetTable.getName().toLowerCase(Locale.ROOT));
                copyTable(sourceDb, sourceTable, targetDb, targetTable);
            }
        } catch (Exception e) {
            if (disableForeignKeys) {
                try {
                    toggleForeignKeys(targetSnapshot, true);
                } catch (Exception nested) {
                    e.addSuppressed(nested);
                }
            }

            if (disableTriggers) {
                try {
                    toggleTriggers(targetSnapshot, true);
                } catch (Exception nested) {
                    e.addSuppressed(nested);
                }
            }

            throw e;
        }

        if (disableForeignKeys) {
            toggleForeignKeys(targetSnapshot, true);
        }

        if (disableForeignKeys) {
            toggleTriggers(targetSnapshot, true);
        }
    }

    private static final Set<String> TIMESTAMP_TYPES = Set.of(
        "oracle.sql.TIMESTAMP"
    );

    private void copyTable(Database source, Table sourceTable,
                   Database target, Table targetTable) throws LiquibaseException {
        final var insertSql = insertSqlForTable(targetTable);
        if (logSql) {
            SQL_LOG.info("{}", insertSql);
        }

        PreparedStatementFactory sourceStmtFactory = new PreparedStatementFactory((JdbcConnection) source.getConnection());
        PreparedStatementFactory targetStmtFactory = new PreparedStatementFactory((JdbcConnection) target.getConnection());
        final var columns = targetTable.getColumns();
        boolean crossVendor = !target.getShortName().equals(source.getShortName());

        try (var count = sourceStmtFactory.create("SELECT COUNT(*) FROM " + sourceTable.getName());
             var select = sourceStmtFactory.create("SELECT * FROM " + sourceTable.getName());
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
                for (var i = 0; i < columns.size(); i++) {
                    var c = columns.get(i);
                    var val = rs.getObject(c.getName());
                    if (val == null) {
                        insert.setObject(i + 1, null);
                        continue;
                    }

                    if (val instanceof Clob clob) {
                        StringBuilder sb = new StringBuilder();
                        try (var reader = clob.getCharacterStream()) {
                            char[] buf = new char[4096];
                            int n;
                            while ((n = reader.read(buf)) > 0) {
                                sb.append(buf, 0, n);
                            }
                        } catch (IOException e) {
                            throw new DatabaseException(e);
                        }

                        val = sb.toString();
                    } else if (val instanceof Blob blob) {
                        ByteArrayOutputStream os = new ByteArrayOutputStream();
                        try (var is = blob.getBinaryStream()) {
                            byte[] buf = new byte[4096];
                            int n;
                            while ((n = is.read(buf)) > 0) {
                                os.write(buf, 0, n);
                            }
                        } catch (IOException e) {
                            throw new DatabaseException(e);
                        }
                        val = os.toByteArray();
                    } else if (crossVendor) {
                        if (TIMESTAMP_TYPES.contains(val.getClass().getName())) {
                            val = Timestamp.valueOf(val.toString());
                        } else if (val instanceof Number n && "bool".equals(c.getType().getTypeName())) {
                            val = n.longValue() != 0;
                        } else if (val instanceof Timestamp ts) {
                            if (ts.getClass() != Timestamp.class) {
                                var tmp = new Timestamp(ts.getTime());
                                tmp.setNanos(ts.getNanos());
                                val = tmp;
                            }
                        } else if (val instanceof Date d && d.getClass() != Date.class) {
                            val = new Date(d.getTime());
                        } else if (!(val instanceof String
                                     || val instanceof Number
                                     || val instanceof Character
                                     || val instanceof Date
                                     || val instanceof Temporal
                                     || val instanceof Boolean
                                     || val instanceof byte[]
                                     || val instanceof char[])) {
                            val = val.toString();
                        }
                    }
                    insert.setObject(i + 1, val);
                }
                row++;
                insert.addBatch();
                if (++rowsInBatch == batchSize) {
                    rowsInBatch = 0;
                    LOG.info("Loading {} {}/{} rows ({}%)", targetTable.getName(), row, total, 100 * row / total);
                    insert.executeBatch();
                    target.commit();
                }
            }

            if (rowsInBatch != 0) {
                LOG.info("Loading {} {}/{} rows ({}%)", targetTable.getName(), row, total, 100 * row / total);
                insert.executeBatch();
                target.commit();
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private void truncateTable(Database db, Table table) throws LiquibaseException {
        final SqlStatement stmt;
        if ("oracle".equals(db.getShortName())) {
            stmt = new RawSqlStatement("TRUNCATE TABLE " + table.getName() + " DROP ALL STORAGE CASCADE");
        } else if ("h2".equals(db.getShortName()) && disableForeignKeys) {
            stmt = new RawSqlStatement("TRUNCATE TABLE " + table.getName());
        } else if ("postgresql".equals(db.getShortName())) {
            stmt = new RawSqlStatement("TRUNCATE TABLE " + table.getName() + " CASCADE");
        } else {
            stmt = new RawSqlStatement("DELETE FROM " + table.getName());
        }

        db.execute(new SqlStatement[]{ stmt }, List.of());
        db.commit();
    }

    private String insertSqlForTable(Table table) {
        var columns = table.getColumns();
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
            sql.append(i == 0 ? "?": ",?");
        }
        sql.append(")");
        return sql.toString();
    }

    private void toggleForeignKeys(DatabaseSnapshot snapshot, boolean enable) throws LiquibaseException {
        List<SqlStatement> stmts = new ArrayList<>();
        var db = snapshot.getDatabase();
        if ("h2".equals(db.getShortName())) {
            stmts.add(new RawSqlStatement("SET REFERENTIAL_INTEGRITY " + enable));
        } else if ("oracle".equals(db.getShortName())) {
            for (var fk : snapshot.get(ForeignKey.class)) {
                var ddl = String.format("ALTER TABLE \"%s\" %s CONSTRAINT \"%s\"",
                    fk.getForeignKeyTable().getName(),
                    enable ? "ENABLE" : "DISABLE",
                    fk.getName());
                stmts.add(new RawSqlStatement(ddl));
            }
        } else if (Set.of("mysql", "mariadb").contains(db.getShortName())) {
            stmts.add(new RawSqlStatement("SET FOREIGN_KEY_CHECKS=" + (enable ? "1" : "0")));
        } else if ("postgresql".equals(db.getShortName())) {
            // TODO: only if superuser
            stmts.add(new RawSqlStatement("SET session_replication_role = " + (enable ? "'origin'" : "'replica'")));
            /*
            var processedTables = new HashSet<String>();
            for (var fk : snapshot.get(ForeignKey.class)) {
                var table = fk.getForeignKeyTable().getName();
                if (processedTables.add(table)) {
                    var ddl = String.format("ALTER TABLE %s %s TRIGGER ALL",
                        table, enable ? "ENABLE" : "DISABLE");
                    stmts.add(new RawSqlStatement(ddl));
                }
            }
            */
        } else {
            throw new DatabaseException("Unsupported database: " + db.getShortName());
        }

        LOG.info("Will {} foreign key checks in {}", enable ? "enable" : "disable", db);

        withoutSqlLogging(() -> {
            db.execute(stmts.toArray(SqlStatement[]::new), List.of());
            db.commit();
        });
    }

    private void toggleTriggers(DatabaseSnapshot snapshot, boolean enable) throws LiquibaseException {
        // TODO: keep track of triggers that were disabled
        List<SqlStatement> stmts = new ArrayList<>();
        var db = snapshot.getDatabase();
        if ("oracle".equals(db.getShortName())) {
            String ddl = """
                    BEGIN
                        FOR r_trigger IN (SELECT TRIGGER_NAME FROM USER_TRIGGERS)
                        LOOP
                            EXECUTE IMMEDIATE ('ALTER TRIGGER ' || r_trigger.TRIGGER_NAME || ' %s');
                        END LOOP;
                    END;
                    """.formatted(enable ? "ENABLE" : "DISABLE");
            stmts.add(new RawSqlStatement(ddl));
        }

        if (!stmts.isEmpty()) {
            LOG.info("Will {} triggers in {}", enable ? "enable" : "disable", db);
            withoutSqlLogging(() -> {
                db.execute(stmts.toArray(SqlStatement[]::new), List.of());
                db.commit();
            });
        }
    }

    private void runChangelog(Database targetDb) throws LiquibaseException {
        var liquibase = new Liquibase(resolvedChangelog, resourceAccessor, targetDb);
        liquibase.update(tag, new Contexts(contexts), new LabelExpression(labelFilter));
        tables.getExclude().addAll(Arrays.asList("DATABASECHANGELOG", "DATABASECHANGELOGLOCK"));
    }

    private void generateChangeLog(Database sourceDb, Database targetDb) throws LiquibaseException {
        var snapshotTypes = new HashSet<>(DatabaseObjectFactory.getInstance().getStandardTypes());
        snapshotTypes.remove(Schema.class);
        snapshotTypes.remove(Catalog.class);
        var catalogAndSchema = sourceDb.getDefaultSchema();
        SchemaComparison[] schemaComparisons = { new SchemaComparison(catalogAndSchema, catalogAndSchema) };
        CompareControl compareControl = new CompareControl(schemaComparisons, snapshotTypes);

        Path changeLogPath = Paths.get("changelog.auto.xml").toAbsolutePath();
        try {
            try (var os = Files.newOutputStream(changeLogPath)) {
                var command = new CommandScope(GenerateChangelogCommandStep.COMMAND_NAME[0])
                    .addArgumentValue(GenerateChangelogCommandStep.CHANGELOG_FILE_ARG, changeLogPath.toString())
                    .addArgumentValue(GenerateChangelogCommandStep.OVERWRITE_OUTPUT_FILE_ARG, true)
                    .addArgumentValue(GenerateChangelogCommandStep.AUTHOR_ARG, "copydb")
                    .addArgumentValue(PreCompareCommandStep.COMPARE_CONTROL_ARG, compareControl)
                    .addArgumentValue(DbUrlConnectionCommandStep.DATABASE_ARG, sourceDb)
                    .addArgumentValue(DbUrlConnectionCommandStep.DATABASE_ARG, sourceDb)
                    .addArgumentValue(PreCompareCommandStep.SNAPSHOT_TYPES_ARG, snapshotTypes.toArray(Class[]::new))
                    .setOutput(os);
                command.execute();
            }

            fixupAutoChangeLog(sourceDb, targetDb, changeLogPath);
            resolvedChangelog = changeLogPath.getFileName().toString();
            resourceAccessor = new DirectoryResourceAccessor(changeLogPath.getParent());
        } catch (IOException e) {
            throw new LiquibaseException("Could not create changelog");
        }
    }

    private void fixupAutoChangeLog(Database sourceDb, Database targetDb, Path path) throws IOException, LiquibaseException {
        var accessor = new DirectoryResourceAccessor(path.getParent());
        var parser = ChangeLogParserFactory.getInstance().getParser(path.getFileName().toString(), accessor);
        var changeLog = parser.parse(path.getFileName().toString(), new ChangeLogParameters(), accessor);

        boolean crossDb = !sourceDb.getShortName().equals(targetDb.getShortName());
        boolean fromOracle = "oracle".equals(sourceDb.getShortName());
        var changeSets = new ArrayList<ChangeSet>();
        int changeSetIndex = 0;
        var sourceSequenceMaxMax = DB_TYPE_SEQUENCE_MAX_MAX.get(sourceDb.getShortName());
        var targetSequenceMaxMax = DB_TYPE_SEQUENCE_MAX_MAX.get(targetDb.getShortName());
        for (var changeSet : changeLog.getChangeSets()) {
            var outputChangeSet = new ChangeSet(String.valueOf(++changeSetIndex), changeSet.getAuthor(),
                changeSet.isAlwaysRun(), changeSet.isRunOnChange(),
                changeSet.getFilePath(),
                (String)null, (String)null,
                (ObjectQuotingStrategy)null, (DatabaseChangeLog)null);
            for (var change : changeSet.getChanges()) {
                if (change instanceof CreateSequenceChange create) {
                    var max = create.getMaxValue();
                    if (max != null) {
                        if (max.equals(sourceSequenceMaxMax)) {
                            create.setMaxValue(null);
                        } else if (targetSequenceMaxMax != null && max.compareTo(targetSequenceMaxMax) > 0) {
                            create.setMaxValue(targetSequenceMaxMax);
                        }
                    }
                } else if (change instanceof AddUniqueConstraintChange u) {
                    if (Objects.equals(u.getConstraintName(), u.getForIndexName())) {
                        u.setForIndexName(null);
                    } else if (fromOracle && u.getConstraintName() != null && u.getConstraintName().startsWith("SYS_")) {
                        u.setConstraintName(null);
                    }
                } else if (crossDb && fromOracle && change instanceof CreateTableChange create) {
                    for (var col : create.getColumns()) {
                        var type = col.getType();
                        if (type.matches("NUMBER\\((19|38)(, 0)?\\)")) {
                            col.setType("BIGINT");
                        } else if (type.matches("NUMBER\\(1(, 0)?\\)")) {
                            col.setType("BOOLEAN");
                        } else {
                            type = type.replace("VARCHAR2(", "VARCHAR(");
                            type = type.replaceAll(" (BYTE|CHAR)\\)", ")");
                            col.setType(type);
                        }

                        var computed = col.getDefaultValueComputed();
                        if (computed != null) {
                            var val = computed.getValue().replaceAll("\\bSYSDATE\\b", "CURRENT_TIMESTAMP");
                            col.setDefaultValueComputed(new DatabaseFunction(computed.getSchemaName(), val));
                        }
                    }
                } else if (change instanceof CreateIndexChange create) {
                    if (crossDb && fromOracle) {
                        var cols = create.getColumns();
                        var last = cols.get(cols.size() - 1);
                        if (last.getComputed() != null && last.getComputed() && last.getName().matches("[01]")) {
                            // Way of forcing Oracle to index nulls
                            cols.remove(cols.size() - 1);
                        }
                    }
                }
                outputChangeSet.addChange(change);
            }
            changeSets.add(outputChangeSet);
        }

        try (var os = Files.newOutputStream(path)) {
            var serializer = new XMLChangeLogSerializer();
            serializer.write(changeSets, os);
        }
    }

    private DatabaseConnection getDatabaseConnection(JdbcProperties properties) throws DatabaseException {
        var dbFactory = DatabaseFactory.getInstance();
        return dbFactory.openConnection(properties.getUrl(), properties.getUsername(), properties.getPassword(), null, resourceAccessor);
    }

}
