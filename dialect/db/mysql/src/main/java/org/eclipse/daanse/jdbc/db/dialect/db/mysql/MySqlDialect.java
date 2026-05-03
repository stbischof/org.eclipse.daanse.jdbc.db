/*
 * Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
 *
 * For more information please visit the Project: Hitachi Vantara - Mondrian
 *
 * ---- All changes after Fork in 2023 ------------------------
 *
 * Project: Eclipse daanse
 *
 * Copyright (c) 2023 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors after Fork in 2023:
 *   SmartCity Jena - initial adapt parts of Syntax.class
 *   Stefan Bischof (bipolis.org) - initial
 */
package org.eclipse.daanse.jdbc.db.dialect.db.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.eclipse.daanse.jdbc.db.api.meta.IndexInfo;
import org.eclipse.daanse.jdbc.db.api.meta.IndexInfoItem;
import org.eclipse.daanse.jdbc.db.api.schema.CheckConstraint;
import org.eclipse.daanse.jdbc.db.api.schema.ColumnMetaData;
import org.eclipse.daanse.jdbc.db.api.schema.ColumnReference;
import org.eclipse.daanse.jdbc.db.api.schema.Function;
import org.eclipse.daanse.jdbc.db.api.schema.FunctionColumn;
import org.eclipse.daanse.jdbc.db.api.schema.FunctionReference;
import org.eclipse.daanse.jdbc.db.api.schema.ImportedKey;
import org.eclipse.daanse.jdbc.db.api.schema.Partition;
import org.eclipse.daanse.jdbc.db.api.schema.PartitionMethod;
import org.eclipse.daanse.jdbc.db.api.schema.PrimaryKey;
import org.eclipse.daanse.jdbc.db.api.schema.Procedure;
import org.eclipse.daanse.jdbc.db.api.schema.ProcedureColumn;
import org.eclipse.daanse.jdbc.db.api.schema.ProcedureReference;
import org.eclipse.daanse.jdbc.db.api.schema.SchemaReference;
import org.eclipse.daanse.jdbc.db.api.schema.Sequence;
import org.eclipse.daanse.jdbc.db.api.schema.TableReference;
import org.eclipse.daanse.jdbc.db.api.schema.Trigger;
import org.eclipse.daanse.jdbc.db.api.schema.Trigger.TriggerEvent;
import org.eclipse.daanse.jdbc.db.api.schema.Trigger.TriggerTiming;
import org.eclipse.daanse.jdbc.db.api.schema.TriggerReference;
import org.eclipse.daanse.jdbc.db.api.schema.UniqueConstraint;
import org.eclipse.daanse.jdbc.db.api.schema.ViewDefinition;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.BitOperation;
import org.eclipse.daanse.jdbc.db.dialect.api.sql.OrderedColumn;
import org.eclipse.daanse.jdbc.db.dialect.db.common.AbstractJdbcDialect;
import org.eclipse.daanse.jdbc.db.dialect.db.common.DialectUtil;
import org.eclipse.daanse.jdbc.db.record.schema.CheckConstraintRecord;
import org.eclipse.daanse.jdbc.db.record.schema.FunctionColumnRecord;
import org.eclipse.daanse.jdbc.db.record.schema.FunctionRecord;
import org.eclipse.daanse.jdbc.db.record.schema.ImportedKeyRecord;
import org.eclipse.daanse.jdbc.db.record.schema.IndexInfoItemRecord;
import org.eclipse.daanse.jdbc.db.record.schema.IndexInfoRecord;
import org.eclipse.daanse.jdbc.db.record.schema.PartitionRecord;
import org.eclipse.daanse.jdbc.db.record.schema.PrimaryKeyRecord;
import org.eclipse.daanse.jdbc.db.record.schema.ProcedureColumnRecord;
import org.eclipse.daanse.jdbc.db.record.schema.ProcedureRecord;
import org.eclipse.daanse.jdbc.db.record.schema.TriggerRecord;
import org.eclipse.daanse.jdbc.db.record.schema.UniqueConstraintRecord;
import org.eclipse.daanse.jdbc.db.record.schema.ViewDefinitionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jhyde
 * @since Nov 23, 2008
 */
public class MySqlDialect extends AbstractJdbcDialect {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlDialect.class);
    private static final String SUPPORTED_PRODUCT_NAME = "MYSQL";

    private volatile org.eclipse.daanse.jdbc.db.dialect.api.generator.PaginationGenerator cachedPaginationGenerator;
    private volatile org.eclipse.daanse.jdbc.db.dialect.api.generator.MergeGenerator cachedMergeGenerator;
    private volatile org.eclipse.daanse.jdbc.db.dialect.api.generator.CteGenerator cachedCteGenerator;

    /** JDBC-free constructor for SQL generation. Uses MySQL backtick quoting. */
    public MySqlDialect() {
        super(org.eclipse.daanse.jdbc.db.dialect.api.DialectInitData.ansiDefaults().withQuoteIdentifierString("`"));
    }

    /** Construct from a captured snapshot — the canonical entry point. */
    public MySqlDialect(org.eclipse.daanse.jdbc.db.dialect.api.DialectInitData init) {
        super(init);
    }

    public static boolean looksLikeInfobright(org.eclipse.daanse.jdbc.db.dialect.api.DialectInitData init) {
        String version = init.productVersion();
        return version != null && version.toLowerCase(java.util.Locale.ROOT).contains("infobright");
    }

    /** MySQL/MariaDB: {@code LIMIT [offset,] count} — both forms accepted. */
    @Override
    public org.eclipse.daanse.jdbc.db.dialect.api.generator.PaginationGenerator paginationGenerator() {
        var local = cachedPaginationGenerator;
        if (local != null)
            return local;
        local = new org.eclipse.daanse.jdbc.db.dialect.api.generator.PaginationGenerator() {
            @Override
            public String paginate(java.util.OptionalLong limit, java.util.OptionalLong offset) {
                StringBuilder sb = new StringBuilder();
                if (limit.isPresent()) {
                    if (limit.getAsLong() < 0)
                        throw new IllegalArgumentException("limit must be >= 0");
                    sb.append(" LIMIT ");
                    if (offset.isPresent()) {
                        if (offset.getAsLong() < 0)
                            throw new IllegalArgumentException("offset must be >= 0");
                        sb.append(offset.getAsLong()).append(", ");
                    }
                    sb.append(limit.getAsLong());
                } else if (offset.isPresent()) {
                    // MySQL has no syntax for OFFSET without LIMIT — use the documented huge-limit
                    // trick.
                    if (offset.getAsLong() < 0)
                        throw new IllegalArgumentException("offset must be >= 0");
                    sb.append(" LIMIT ").append(offset.getAsLong()).append(", 18446744073709551615");
                }
                return sb.toString();
            }
        };
        cachedPaginationGenerator = local;
        return local;
    }

    /**
     * MySQL/MariaDB:
     * {@code INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col), ...}.
     */
    @Override
    public org.eclipse.daanse.jdbc.db.dialect.api.generator.MergeGenerator mergeGenerator() {
        var local = cachedMergeGenerator;
        if (local != null)
            return local;
        local = new org.eclipse.daanse.jdbc.db.dialect.api.generator.MergeGenerator() {
            @Override
            public boolean supportsMerge() {
                return true;
            }

            @Override
            public java.util.Optional<String> upsert(UpsertSpec spec, java.util.List<String> values) {
                if (values == null || values.size() != spec.insertColumns().size()) {
                    throw new IllegalArgumentException("values must match insertColumns in length");
                }
                StringBuilder sb = new StringBuilder("INSERT INTO ").append(qualified(spec.target())).append(" (");
                appendQuotedCsv(sb, spec.insertColumns());
                sb.append(") VALUES (");
                for (int i = 0; i < values.size(); i++) {
                    if (i > 0)
                        sb.append(", ");
                    sb.append(values.get(i));
                }
                sb.append(")");
                if (!spec.updateColumns().isEmpty()) {
                    sb.append(" ON DUPLICATE KEY UPDATE ");
                    boolean first = true;
                    for (String c : spec.updateColumns()) {
                        if (!first)
                            sb.append(", ");
                        sb.append(quoteIdentifier(c)).append(" = VALUES(").append(quoteIdentifier(c)).append(")");
                        first = false;
                    }
                }
                return java.util.Optional.of(sb.toString());
            }
        };
        cachedMergeGenerator = local;
        return local;
    }

    /** MySQL silently ignores {@code CASCADE} on {@code DROP TABLE}. */
    @Override
    public boolean supportsDropTableCascade() {
        return false;
    }

    /** MySQL has no SQL sequences — use {@code AUTO_INCREMENT} columns. */
    @Override
    public boolean supportsSequences() {
        return false;
    }

    /**
     * MySQL/MariaDB: index names are table-scoped —
     * {@code DROP INDEX name ON table}.
     */
    @Override
    public boolean dropIndexRequiresTable() {
        return true;
    }

    /**
     * MySQL rejects {@code IF NOT EXISTS} on {@code CREATE INDEX} (MariaDB accepts
     * it).
     */
    @Override
    public boolean supportsCreateIndexIfNotExists() {
        return false;
    }

    @Override
    public org.eclipse.daanse.jdbc.db.dialect.api.generator.CteGenerator cteGenerator() {
        var local = cachedCteGenerator;
        if (local != null)
            return local;
        boolean supports = org.eclipse.daanse.jdbc.db.dialect.api.DialectVersion.UNKNOWN.equals(dialectVersion)
                || dialectVersion.atLeast(8, 0);
        local = new org.eclipse.daanse.jdbc.db.dialect.api.generator.CteGenerator() {
            @Override
            public boolean supportsRecursiveCte() {
                return supports;
            }
        };
        cachedCteGenerator = local;
        return local;
    }

    /**
     * MySQL rejects {@code IF EXISTS} on {@code DROP INDEX} (MariaDB accepts it).
     */
    @Override
    public boolean supportsDropIndexIfExists() {
        return false;
    }

    /** MySQL rejects {@code IF EXISTS} on {@code ALTER TABLE … DROP CONSTRAINT}. */
    @Override
    public boolean supportsDropConstraintIfExists() {
        return false;
    }

    /**
     * @param metaData DatabaseMetaData
     * @return Whether this is Infobright
     */
    public static boolean isInfobright(DatabaseMetaData metaData) {
        // Infobright detection is currently disabled. A separate Infobright dialect
        // or a configurable flag could be added if Infobright support is needed.
        // Detection would require querying for the BRIGHTHOUSE engine presence.
        return false;
    }

    @Override
    public void appendHintsAfterFromClause(StringBuilder buf, Map<String, String> hints) {
        if (hints != null) {
            String forcedIndex = hints.get("force_index");
            if (forcedIndex != null) {
                buf.append(" FORCE INDEX (");
                buf.append(forcedIndex);
                buf.append(")");
            }
        }
    }

    @Override
    public boolean requiresAliasForFromQuery() {
        return true;
    }

    @Override
    public boolean allowsFromQuery() {
        // MySQL before 4.0 does not allow FROM
        // subqueries in the FROM clause.
        return productVersion.compareTo("4.") >= 0;
    }

    @Override
    public boolean allowsCompoundCountDistinct() {
        return true;
    }

    @Override
    public void quoteStringLiteral(StringBuilder buf, String s) {
        // Go beyond standard singleQuoteString; also quote backslash.
        buf.append('\'');
        String s0 = s.replace("'", "''");
        String s1 = s0.replace("\\", "\\\\");
        buf.append(s1);
        buf.append('\'');
    }

    @Override
    public void quoteBooleanLiteral(StringBuilder buf, String value) {
        if (!value.equalsIgnoreCase("1") && !(value.equalsIgnoreCase("0"))) {
            super.quoteBooleanLiteral(buf, value);
        } else {
            buf.append(value);
        }
    }

    @Override
    public StringBuilder generateInline(List<String> columnNames, List<String> columnTypes, List<String[]> valueList) {
        return generateInlineGeneric(columnNames, columnTypes, valueList, null, false);
    }

    @Override
    public StringBuilder generateOrderByNulls(CharSequence expr, boolean ascending, boolean collateNullsLast) {
        // In MYSQL, Null values are worth negative infinity.
        return DialectUtil.generateOrderByNullsWithIsnull(expr, ascending, collateNullsLast);
    }

    @Override
    public boolean requiresHavingAlias() {
        return true;
    }

    @Override
    public boolean supportsMultiValueInExpr() {
        return true;
    }

    private enum Scope {
        SESSION, GLOBAL
    }

    @Override
    public boolean allowsRegularExpressionInWhereClause() {
        return true;
    }

    @Override
    public Optional<String> generateRegularExpression(String source, String javaRegex) {
        try {
            Pattern.compile(javaRegex);
        } catch (PatternSyntaxException e) {
            // Not a valid Java regex. Too risky to continue.
            return Optional.empty();
        }

        // We might have to use case-insensitive matching
        javaRegex = DialectUtil.cleanUnicodeAwareCaseFlag(javaRegex);
        StringBuilder mappedFlags = new StringBuilder();
        String[][] mapping = new String[][] { { "i", "i" } };
        javaRegex = extractEmbeddedFlags(javaRegex, mapping, mappedFlags);
        boolean caseSensitive = true;
        if (mappedFlags.toString().contains("i")) {
            caseSensitive = false;
        }
        final Matcher escapeMatcher = DialectUtil.ESCAPE_PATTERN.matcher(javaRegex);
        while (escapeMatcher.find()) {
            javaRegex = javaRegex.replace(escapeMatcher.group(1), escapeMatcher.group(2));
        }
        final StringBuilder sb = new StringBuilder();

        // Now build the string.
        sb.append(source);
        sb.append(" IS NOT NULL AND ");
        if (caseSensitive) {
            sb.append(source);
        } else {
            sb.append("UPPER(");
            sb.append(source);
            sb.append(")");
        }
        sb.append(" REGEXP ");
        if (caseSensitive) {
            quoteStringLiteral(sb, javaRegex);
        } else {
            quoteStringLiteral(sb, javaRegex.toUpperCase());
        }
        return Optional.of(sb.toString());
    }

    /**
     * @return true when MySQL version is 5.7 or larger
     */
    @Override
    public boolean requiresOrderByAlias() {
        return productVersion.compareTo("5.7") >= 0;
    }

    @Override
    public String name() {
        return SUPPORTED_PRODUCT_NAME.toLowerCase();
    }

    @Override
    public org.eclipse.daanse.jdbc.db.dialect.api.IdentifierCaseFolding caseFolding() {
        return org.eclipse.daanse.jdbc.db.dialect.api.IdentifierCaseFolding.PRESERVE;
    }

    // Unified BitOperation methods

    @Override
    public java.util.Optional<String> generateBitAggregation(BitOperation operation, CharSequence operand) {
        StringBuilder buf = new StringBuilder(64);
        StringBuilder result = switch (operation) {
        case AND -> buf.append("BIT_AND(").append(operand).append(")");
        case OR -> buf.append("BIT_OR(").append(operand).append(")");
        case XOR -> buf.append("BIT_XOR(").append(operand).append(")");
        case NAND -> buf.append("NOT(BIT_AND(").append(operand).append("))");
        case NOR -> buf.append("NOT(BIT_OR(").append(operand).append("))");
        case NXOR -> buf.append("NOT(BIT_XOR(").append(operand).append("))");
        };
        return java.util.Optional.of(result.toString());
    }

    @Override
    public boolean supportsBitAggregation(BitOperation operation) {
        return true; // MySQL supports all bit operations
    }

    @Override
    public java.util.Optional<String> generatePercentileDisc(double percentile, boolean desc, String tableName,
            String columnName) {
        return java.util.Optional
                .of((buildPercentileFunction("PERCENTILE_DISC", percentile, desc, tableName, columnName)).toString());
    }

    @Override
    public java.util.Optional<String> generatePercentileCont(double percentile, boolean desc, String tableName,
            String columnName) {
        return java.util.Optional
                .of((buildPercentileFunction("PERCENTILE_CONT", percentile, desc, tableName, columnName)).toString());
    }

    @Override
    public boolean supportsPercentileDisc() {
        return productVersion.compareTo("8.0") >= 0;
    }

    @Override
    public boolean supportsPercentileCont() {
        return productVersion.compareTo("8.0") >= 0;
    }

    @Override
    public java.util.Optional<String> generateListAgg(CharSequence operand, boolean distinct, String separator,
            String coalesce, String onOverflowTruncate, List<OrderedColumn> columns) {
        StringBuilder buf = new StringBuilder(64);
        buf.append("GROUP_CONCAT");
        buf.append("( ");
        if (distinct) {
            buf.append("DISTINCT ");
        }
        buf.append(operand);
        if (columns != null && !columns.isEmpty()) {
            buf.append(" ORDER BY ");
            buf.append(buildOrderedColumnsClause(columns));
        }
        if (separator != null) {
            buf.append(" SEPARATOR '").append(separator).append("'");
        }
        buf.append(")");
        // GROUP_CONCAT(DISTINCT cate_id ORDER BY cate_id ASC SEPARATOR ' ')
        return java.util.Optional.of((buf).toString());
    }

    @Override
    public java.util.Optional<String> generateNthValueAgg(CharSequence operand, boolean ignoreNulls, Integer n,
            List<OrderedColumn> columns) {
        return java.util.Optional
                .of((buildNthValueFunction("NTH_VALUE", operand, ignoreNulls, n, columns, false)).toString());
    }

    @Override
    public boolean supportsNthValue() {
        return true;
    }

    @Override
    public boolean supportsListAgg() {
        return true;
    }

    @Override
    public List<Trigger> getAllTriggers(Connection connection, String catalog, String schema) throws SQLException {
        String sql = """
                SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION,
                       ACTION_STATEMENT, ACTION_ORIENTATION
                FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ?
                ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME
                """;
        String schemaName = resolveSchema(schema, connection);
        List<Trigger> triggers = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    triggers.add(readTrigger(rs, schemaName));
                }
            }
        }
        return List.copyOf(triggers);
    }

    @Override
    public List<Trigger> getTriggers(Connection connection, String catalog, String schema, String tableName)
            throws SQLException {
        String sql = """
                SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION,
                       ACTION_STATEMENT, ACTION_ORIENTATION
                FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?
                ORDER BY TRIGGER_NAME
                """;
        String schemaName = resolveSchema(schema, connection);
        List<Trigger> triggers = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    triggers.add(readTrigger(rs, schemaName));
                }
            }
        }
        return List.copyOf(triggers);
    }

    @Override
    public List<Sequence> getAllSequences(Connection connection, String catalog, String schema) throws SQLException {
        // MySQL does NOT support sequences
        return List.of();
    }

    @Override
    public List<Partition> getAllPartitions(Connection connection, String catalog, String schema) throws SQLException {
        // INFORMATION_SCHEMA.PARTITIONS returns one row per non-partitioned table with
        // PARTITION_NAME=NULL, plus one row per (partition, sub-partition) for
        // partitioned tables.
        // We filter out the PARTITION_NAME IS NULL rows.
        String sql = """
                SELECT TABLE_NAME, PARTITION_NAME, SUBPARTITION_NAME,
                       PARTITION_METHOD, SUBPARTITION_METHOD,
                       PARTITION_EXPRESSION, SUBPARTITION_EXPRESSION,
                       PARTITION_DESCRIPTION,
                       PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION,
                       TABLE_ROWS
                FROM information_schema.PARTITIONS
                WHERE TABLE_SCHEMA = ? AND PARTITION_NAME IS NOT NULL
                ORDER BY TABLE_NAME, PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION
                """;
        String schemaName = resolveSchema(schema, connection);
        List<Partition> partitions = new ArrayList<>();
        Optional<SchemaReference> oSchema = Optional.of(new SchemaReference(Optional.empty(), schemaName));
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    String partitionName = rs.getString("PARTITION_NAME");
                    String subPartitionName = rs.getString("SUBPARTITION_NAME");
                    boolean isSub = subPartitionName != null;

                    String name = isSub ? subPartitionName : partitionName;
                    String methodStr = rs.getString(isSub ? "SUBPARTITION_METHOD" : "PARTITION_METHOD");
                    PartitionMethod method = mapPartitionMethod(methodStr);
                    String expression = rs.getString(isSub ? "SUBPARTITION_EXPRESSION" : "PARTITION_EXPRESSION");
                    String description = rs.getString("PARTITION_DESCRIPTION");
                    int ordinal = rs.getInt(isSub ? "SUBPARTITION_ORDINAL_POSITION" : "PARTITION_ORDINAL_POSITION");
                    Optional<Integer> oOrdinal = rs.wasNull() ? Optional.empty() : Optional.of(ordinal);
                    long rowCount = rs.getLong("TABLE_ROWS");
                    Optional<Long> oRowCount = rs.wasNull() ? Optional.empty() : Optional.of(rowCount);

                    TableReference tableRef = new TableReference(oSchema, tableName);
                    partitions.add(new PartitionRecord(name, tableRef, oOrdinal, method,
                            expression == null ? Optional.empty() : Optional.of(expression),
                            description == null ? Optional.empty() : Optional.of(description), oRowCount,
                            isSub ? Optional.of(partitionName) : Optional.empty(), Optional.empty(), Optional.empty()));
                }
            }
        } catch (SQLException e) {
            LOGGER.debug("Could not load partitions from INFORMATION_SCHEMA.PARTITIONS: {}", e.getMessage());
            return List.of();
        }
        return List.copyOf(partitions);
    }

    private static PartitionMethod mapPartitionMethod(String raw) {
        if (raw == null) {
            return PartitionMethod.OTHER;
        }
        return switch (raw.toUpperCase(java.util.Locale.ROOT)) {
        case "RANGE", "RANGE COLUMNS" -> PartitionMethod.RANGE;
        case "LIST", "LIST COLUMNS" -> PartitionMethod.LIST;
        case "HASH" -> PartitionMethod.HASH;
        case "KEY" -> PartitionMethod.KEY;
        case "LINEAR HASH" -> PartitionMethod.LINEAR_HASH;
        case "LINEAR KEY" -> PartitionMethod.LINEAR_KEY;
        default -> PartitionMethod.OTHER;
        };
    }

    @Override
    public List<CheckConstraint> getAllCheckConstraints(Connection connection, String catalog, String schema)
            throws SQLException {
        String sql = """
                SELECT cc.CONSTRAINT_NAME, cc.CHECK_CLAUSE, tc.TABLE_NAME
                FROM information_schema.CHECK_CONSTRAINTS cc
                JOIN information_schema.TABLE_CONSTRAINTS tc
                  ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                WHERE cc.CONSTRAINT_SCHEMA = ? AND tc.CONSTRAINT_TYPE = 'CHECK'
                ORDER BY tc.TABLE_NAME, cc.CONSTRAINT_NAME
                """;
        String schemaName = resolveSchema(schema, connection);
        List<CheckConstraint> constraints = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String constraintName = rs.getString("CONSTRAINT_NAME");
                    String checkClause = rs.getString("CHECK_CLAUSE");
                    String tableName = rs.getString("TABLE_NAME");

                    Optional<SchemaReference> oSchema = Optional.of(new SchemaReference(Optional.empty(), schemaName));
                    TableReference tableRef = new TableReference(oSchema, tableName);

                    constraints.add(new CheckConstraintRecord(constraintName, tableRef, checkClause));
                }
            }
        } catch (SQLException e) {
            // CHECK_CONSTRAINTS is only available in MySQL 8.0.16+
            LOGGER.debug("Could not query CHECK_CONSTRAINTS (requires MySQL 8.0.16+)", e);
        }
        return List.copyOf(constraints);
    }

    @Override
    public List<CheckConstraint> getCheckConstraints(Connection connection, String catalog, String schema,
            String tableName) throws SQLException {
        String sql = """
                SELECT cc.CONSTRAINT_NAME, cc.CHECK_CLAUSE, tc.TABLE_NAME
                FROM information_schema.CHECK_CONSTRAINTS cc
                JOIN information_schema.TABLE_CONSTRAINTS tc
                  ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                WHERE cc.CONSTRAINT_SCHEMA = ? AND tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'CHECK'
                ORDER BY cc.CONSTRAINT_NAME
                """;
        String schemaName = resolveSchema(schema, connection);
        List<CheckConstraint> constraints = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String constraintName = rs.getString("CONSTRAINT_NAME");
                    String checkClause = rs.getString("CHECK_CLAUSE");

                    Optional<SchemaReference> oSchema = Optional.of(new SchemaReference(Optional.empty(), schemaName));
                    TableReference tableRef = new TableReference(oSchema, tableName);

                    constraints.add(new CheckConstraintRecord(constraintName, tableRef, checkClause));
                }
            }
        } catch (SQLException e) {
            // CHECK_CONSTRAINTS is only available in MySQL 8.0.16+
            LOGGER.debug("Could not query CHECK_CONSTRAINTS (requires MySQL 8.0.16+)", e);
        }
        return List.copyOf(constraints);
    }

    @Override
    public List<UniqueConstraint> getAllUniqueConstraints(Connection connection, String catalog, String schema)
            throws SQLException {
        String sql = """
                SELECT tc.CONSTRAINT_NAME, tc.TABLE_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION
                FROM information_schema.TABLE_CONSTRAINTS tc
                JOIN information_schema.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE tc.CONSTRAINT_TYPE = 'UNIQUE' AND tc.TABLE_SCHEMA = ?
                ORDER BY tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
                """;
        return readUniqueConstraints(connection, sql, schema, null);
    }

    @Override
    public List<UniqueConstraint> getUniqueConstraints(Connection connection, String catalog, String schema,
            String tableName) throws SQLException {
        String sql = """
                SELECT tc.CONSTRAINT_NAME, tc.TABLE_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION
                FROM information_schema.TABLE_CONSTRAINTS tc
                JOIN information_schema.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE tc.CONSTRAINT_TYPE = 'UNIQUE' AND tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
                ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
                """;
        return readUniqueConstraints(connection, sql, schema, tableName);
    }

    @Override
    public Optional<List<PrimaryKey>> getAllPrimaryKeys(Connection connection, String catalog, String schema)
            throws SQLException {
        String sql = """
                SELECT tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION
                FROM information_schema.TABLE_CONSTRAINTS tc
                JOIN information_schema.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_SCHEMA = ?
                ORDER BY tc.TABLE_NAME, kcu.ORDINAL_POSITION
                """;
        String schemaName = resolveSchema(schema, connection);
        Map<String, PkBuilder> pkMap = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String constraintName = rs.getString("CONSTRAINT_NAME");
                    String tableName = rs.getString("TABLE_NAME");
                    String columnName = rs.getString("COLUMN_NAME");

                    String key = tableName + "." + constraintName;
                    pkMap.computeIfAbsent(key, k -> new PkBuilder(tableName, constraintName, schemaName))
                            .addColumn(columnName);
                }
            }
        }
        List<PrimaryKey> result = new ArrayList<>();
        for (PkBuilder builder : pkMap.values()) {
            result.add(builder.build());
        }
        return Optional.of(List.copyOf(result));
    }

    @Override
    public Optional<List<ImportedKey>> getAllImportedKeys(Connection connection, String catalog, String schema)
            throws SQLException {
        String sql = """
                SELECT tc.CONSTRAINT_NAME AS FK_NAME, kcu.TABLE_NAME AS FK_TABLE, kcu.COLUMN_NAME AS FK_COLUMN,
                       kcu.REFERENCED_TABLE_NAME AS PK_TABLE, kcu.REFERENCED_COLUMN_NAME AS PK_COLUMN,
                       kcu.ORDINAL_POSITION AS KEY_SEQ, rc.DELETE_RULE, rc.UPDATE_RULE
                FROM information_schema.TABLE_CONSTRAINTS tc
                JOIN information_schema.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_NAME = kcu.TABLE_NAME
                JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                  ON tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY' AND tc.TABLE_SCHEMA = ?
                ORDER BY kcu.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
                """;
        String schemaName = resolveSchema(schema, connection);
        List<ImportedKey> importedKeys = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    importedKeys.add(readImportedKey(rs, schemaName));
                }
            }
        }
        return Optional.of(List.copyOf(importedKeys));
    }

    @Override
    public Optional<List<ImportedKey>> getAllExportedKeys(Connection connection, String catalog, String schema)
            throws SQLException {
        // Symmetric to getAllImportedKeys: find FKs that reference tables in this
        // schema.
        String sql = """
                SELECT tc.CONSTRAINT_NAME AS FK_NAME, kcu.TABLE_NAME AS FK_TABLE, kcu.COLUMN_NAME AS FK_COLUMN,
                       kcu.REFERENCED_TABLE_NAME AS PK_TABLE, kcu.REFERENCED_COLUMN_NAME AS PK_COLUMN,
                       kcu.ORDINAL_POSITION AS KEY_SEQ, rc.DELETE_RULE, rc.UPDATE_RULE
                FROM information_schema.TABLE_CONSTRAINTS tc
                JOIN information_schema.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_NAME = kcu.TABLE_NAME
                JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                  ON tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY' AND kcu.REFERENCED_TABLE_SCHEMA = ?
                ORDER BY kcu.REFERENCED_TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
                """;
        String schemaName = resolveSchema(schema, connection);
        List<ImportedKey> exportedKeys = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    exportedKeys.add(readImportedKey(rs, schemaName));
                }
            }
        }
        return Optional.of(List.copyOf(exportedKeys));
    }

    @Override
    public Optional<List<IndexInfo>> getAllIndexInfo(Connection connection, String catalog, String schema)
            throws SQLException {
        String sql = """
                SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE, INDEX_TYPE, CARDINALITY
                FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ?
                ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
                """;
        String schemaName = resolveSchema(schema, connection);
        // Group by TABLE_NAME then by INDEX_NAME
        Map<String, Map<String, List<IndexInfoItem>>> tableIndexMap = new LinkedHashMap<>();
        Map<String, TableReference> tableRefs = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    String indexName = rs.getString("INDEX_NAME");
                    String columnName = rs.getString("COLUMN_NAME");
                    int seqInIndex = rs.getInt("SEQ_IN_INDEX");
                    boolean nonUnique = rs.getBoolean("NON_UNIQUE");
                    String indexType = rs.getString("INDEX_TYPE");
                    long cardinality = rs.getLong("CARDINALITY");

                    IndexInfoItem.IndexType mappedType = mapMySqlIndexType(indexType);

                    TableReference tableRef = tableRefs.computeIfAbsent(tableName, k -> {
                        Optional<SchemaReference> oSchema = Optional
                                .of(new SchemaReference(Optional.empty(), schemaName));
                        return new TableReference(oSchema, k);
                    });

                    Optional<ColumnReference> colRef = Optional.ofNullable(columnName)
                            .map(cn -> new ColumnReference(Optional.of(tableRef), cn));

                    IndexInfoItem item = new IndexInfoItemRecord(Optional.ofNullable(indexName), mappedType, colRef,
                            seqInIndex, Optional.empty(), // MySQL STATISTICS doesn't expose ASC/DESC
                            cardinality, 0L, // pages not available
                            Optional.empty(), // filter condition
                            !nonUnique);

                    tableIndexMap.computeIfAbsent(tableName, k -> new LinkedHashMap<>())
                            .computeIfAbsent(indexName, k -> new ArrayList<>()).add(item);
                }
            }
        }
        List<IndexInfo> result = new ArrayList<>();
        for (Map.Entry<String, Map<String, List<IndexInfoItem>>> tableEntry : tableIndexMap.entrySet()) {
            List<IndexInfoItem> allItems = new ArrayList<>();
            for (List<IndexInfoItem> indexItems : tableEntry.getValue().values()) {
                allItems.addAll(indexItems);
            }
            result.add(new IndexInfoRecord(tableRefs.get(tableEntry.getKey()), List.copyOf(allItems)));
        }
        return Optional.of(List.copyOf(result));
    }

    @Override
    public List<ViewDefinition> getAllViewDefinitions(Connection connection, String catalog, String schema)
            throws SQLException {
        String sql = """
                SELECT TABLE_NAME, VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ?
                ORDER BY TABLE_NAME
                """;
        String schemaName = resolveSchema(schema, connection);
        List<ViewDefinition> views = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    String viewDef = rs.getString("VIEW_DEFINITION");

                    Optional<SchemaReference> oSchema = Optional.of(new SchemaReference(Optional.empty(), schemaName));
                    TableReference viewRef = new TableReference(oSchema, tableName, "VIEW");

                    views.add(new ViewDefinitionRecord(viewRef, Optional.ofNullable(viewDef), Optional.empty()));
                }
            }
        }
        return List.copyOf(views);
    }

    @Override
    public List<Procedure> getAllProcedures(Connection connection, String catalog, String schema) throws SQLException {
        String schemaName = resolveSchema(schema, connection);
        Map<String, List<ProcedureColumn>> paramMap = loadProcedureColumns(connection, schemaName);

        String sql = """
                SELECT ROUTINE_NAME, SPECIFIC_NAME, ROUTINE_COMMENT, ROUTINE_DEFINITION
                FROM information_schema.ROUTINES
                WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = 'PROCEDURE'
                ORDER BY ROUTINE_NAME
                """;
        List<Procedure> procedures = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String routineName = rs.getString("ROUTINE_NAME");
                    String specificName = rs.getString("SPECIFIC_NAME");
                    String remarks = rs.getString("ROUTINE_COMMENT");
                    String body = rs.getString("ROUTINE_DEFINITION");

                    Optional<SchemaReference> oSchema = Optional.of(new SchemaReference(Optional.empty(), schemaName));
                    List<ProcedureColumn> cols = paramMap.getOrDefault(specificName, List.of());

                    Optional<String> fullDef = showCreateRoutine(connection, "PROCEDURE", schemaName, routineName);

                    procedures.add(new ProcedureRecord(new ProcedureReference(oSchema, routineName, specificName),
                            Procedure.ProcedureType.NO_RESULT, Optional.ofNullable(remarks), cols,
                            Optional.ofNullable(body), fullDef, Optional.empty()));
                }
            }
        }
        return List.copyOf(procedures);
    }

    @Override
    public List<Function> getAllFunctions(Connection connection, String catalog, String schema) throws SQLException {
        String schemaName = resolveSchema(schema, connection);
        Map<String, List<FunctionColumn>> paramMap = loadFunctionColumns(connection, schemaName);

        String sql = """
                SELECT ROUTINE_NAME, SPECIFIC_NAME, ROUTINE_COMMENT, ROUTINE_DEFINITION
                FROM information_schema.ROUTINES
                WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = 'FUNCTION'
                ORDER BY ROUTINE_NAME
                """;
        List<Function> functions = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String routineName = rs.getString("ROUTINE_NAME");
                    String specificName = rs.getString("SPECIFIC_NAME");
                    String remarks = rs.getString("ROUTINE_COMMENT");
                    String body = rs.getString("ROUTINE_DEFINITION");

                    Optional<SchemaReference> oSchema = Optional.of(new SchemaReference(Optional.empty(), schemaName));
                    List<FunctionColumn> cols = paramMap.getOrDefault(specificName, List.of());

                    Optional<String> fullDef = showCreateRoutine(connection, "FUNCTION", schemaName, routineName);

                    functions.add(new FunctionRecord(new FunctionReference(oSchema, routineName, specificName),
                            Function.FunctionType.NO_TABLE, Optional.ofNullable(remarks), cols,
                            Optional.ofNullable(body), fullDef, Optional.empty()));
                }
            }
        }
        return List.copyOf(functions);
    }

    private Optional<String> showCreateRoutine(Connection connection, String kind, String schemaName, String name) {
        String quotedSchema = "`" + schemaName.replace("`", "``") + "`";
        String quotedName = "`" + name.replace("`", "``") + "`";
        String sql = "SHOW CREATE " + kind + " " + quotedSchema + "." + quotedName;
        try (PreparedStatement ps = connection.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                String col = "Create " + kind.charAt(0) + kind.substring(1).toLowerCase();
                String def = rs.getString(col);
                return Optional.ofNullable(def);
            }
        } catch (SQLException e) {
            LOGGER.debug("Could not retrieve DDL for {} {}.{}: {}", kind, schemaName, name, e.getMessage());
        }
        return Optional.empty();
    }

    private Map<String, List<ProcedureColumn>> loadProcedureColumns(Connection connection, String schemaName)
            throws SQLException {
        String sql = """
                SELECT SPECIFIC_NAME, PARAMETER_NAME, PARAMETER_MODE, DATA_TYPE,
                       ORDINAL_POSITION, NUMERIC_PRECISION, NUMERIC_SCALE
                FROM information_schema.PARAMETERS
                WHERE SPECIFIC_SCHEMA = ? AND ORDINAL_POSITION > 0
                ORDER BY SPECIFIC_NAME, ORDINAL_POSITION
                """;
        Map<String, List<ProcedureColumn>> result = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String specificName = rs.getString("SPECIFIC_NAME");
                    String paramName = rs.getString("PARAMETER_NAME");
                    String paramMode = rs.getString("PARAMETER_MODE");
                    String dataType = rs.getString("DATA_TYPE");
                    int ordinalPosition = rs.getInt("ORDINAL_POSITION");
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    boolean precisionNull = rs.wasNull();
                    int scale = rs.getInt("NUMERIC_SCALE");
                    boolean scaleNull = rs.wasNull();

                    ProcedureColumn.ColumnType colType = mapProcedureColumnType(paramMode);
                    JDBCType jdbcType = mapMySqlJdbcType(dataType);

                    ProcedureColumn col = new ProcedureColumnRecord(paramName != null ? paramName : "", colType,
                            jdbcType, dataType != null ? dataType : "",
                            precisionNull ? OptionalInt.empty() : OptionalInt.of(precision),
                            scaleNull ? OptionalInt.empty() : OptionalInt.of(scale), OptionalInt.of(10),
                            ProcedureColumn.Nullability.UNKNOWN, Optional.empty(), Optional.empty(), ordinalPosition);

                    result.computeIfAbsent(specificName, k -> new ArrayList<>()).add(col);
                }
            }
        }
        return result;
    }

    private Map<String, List<FunctionColumn>> loadFunctionColumns(Connection connection, String schemaName)
            throws SQLException {
        String sql = """
                SELECT SPECIFIC_NAME, PARAMETER_NAME, PARAMETER_MODE, DATA_TYPE,
                       ORDINAL_POSITION, NUMERIC_PRECISION, NUMERIC_SCALE
                FROM information_schema.PARAMETERS
                WHERE SPECIFIC_SCHEMA = ?
                ORDER BY SPECIFIC_NAME, ORDINAL_POSITION
                """;
        Map<String, List<FunctionColumn>> result = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String specificName = rs.getString("SPECIFIC_NAME");
                    String paramName = rs.getString("PARAMETER_NAME");
                    String paramMode = rs.getString("PARAMETER_MODE");
                    String dataType = rs.getString("DATA_TYPE");
                    int ordinalPosition = rs.getInt("ORDINAL_POSITION");
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    boolean precisionNull = rs.wasNull();
                    int scale = rs.getInt("NUMERIC_SCALE");
                    boolean scaleNull = rs.wasNull();

                    FunctionColumn.ColumnType colType = mapFunctionColumnType(paramMode);
                    JDBCType jdbcType = mapMySqlJdbcType(dataType);

                    FunctionColumn col = new FunctionColumnRecord(paramName != null ? paramName : "", colType, jdbcType,
                            dataType != null ? dataType : "",
                            precisionNull ? OptionalInt.empty() : OptionalInt.of(precision),
                            scaleNull ? OptionalInt.empty() : OptionalInt.of(scale), OptionalInt.of(10),
                            FunctionColumn.Nullability.UNKNOWN, Optional.empty(), OptionalInt.empty(), ordinalPosition);

                    result.computeIfAbsent(specificName, k -> new ArrayList<>()).add(col);
                }
            }
        }
        return result;
    }

    private static ProcedureColumn.ColumnType mapProcedureColumnType(String mode) {
        if (mode == null) {
            return ProcedureColumn.ColumnType.UNKNOWN;
        }
        return switch (mode.toUpperCase()) {
        case "IN" -> ProcedureColumn.ColumnType.IN;
        case "OUT" -> ProcedureColumn.ColumnType.OUT;
        case "INOUT" -> ProcedureColumn.ColumnType.INOUT;
        default -> ProcedureColumn.ColumnType.UNKNOWN;
        };
    }

    private static FunctionColumn.ColumnType mapFunctionColumnType(String mode) {
        if (mode == null) {
            return FunctionColumn.ColumnType.UNKNOWN;
        }
        return switch (mode.toUpperCase()) {
        case "IN" -> FunctionColumn.ColumnType.IN;
        case "OUT" -> FunctionColumn.ColumnType.OUT;
        case "INOUT" -> FunctionColumn.ColumnType.INOUT;
        default -> FunctionColumn.ColumnType.UNKNOWN;
        };
    }

    private static JDBCType mapMySqlJdbcType(String dataType) {
        if (dataType == null) {
            return JDBCType.OTHER;
        }
        try {
            return JDBCType.valueOf(dataType.toUpperCase().replace(" ", "_"));
        } catch (IllegalArgumentException e) {
            return switch (dataType.toUpperCase()) {
            case "INT" -> JDBCType.INTEGER;
            case "MEDIUMINT" -> JDBCType.INTEGER;
            case "TINYINT" -> JDBCType.TINYINT;
            case "BOOL", "BOOLEAN" -> JDBCType.BOOLEAN;
            case "TEXT", "MEDIUMTEXT", "LONGTEXT", "TINYTEXT" -> JDBCType.VARCHAR;
            case "DATETIME" -> JDBCType.TIMESTAMP;
            case "ENUM", "SET", "JSON", "GEOMETRY" -> JDBCType.OTHER;
            default -> JDBCType.OTHER;
            };
        }
    }

    private Trigger readTrigger(ResultSet rs, String schemaName) throws SQLException {
        String triggerName = rs.getString("TRIGGER_NAME");
        String tableName = rs.getString("EVENT_OBJECT_TABLE");
        String actionTiming = rs.getString("ACTION_TIMING");
        String eventManipulation = rs.getString("EVENT_MANIPULATION");
        String actionStatement = rs.getString("ACTION_STATEMENT");
        String actionOrientation = rs.getString("ACTION_ORIENTATION");

        Optional<SchemaReference> oSchema = Optional.of(new SchemaReference(Optional.empty(), schemaName));
        TableReference tableRef = new TableReference(oSchema, tableName);

        TriggerTiming timing = mapTriggerTiming(actionTiming);
        TriggerEvent event = mapTriggerEvent(eventManipulation);

        return new TriggerRecord(new TriggerReference(tableRef, triggerName), timing, event,
                Optional.ofNullable(actionStatement), Optional.empty(), Optional.ofNullable(actionOrientation));
    }

    private ImportedKey readImportedKey(ResultSet rs, String schemaName) throws SQLException {
        String fkName = rs.getString("FK_NAME");
        String fkTable = rs.getString("FK_TABLE");
        String fkColumn = rs.getString("FK_COLUMN");
        String pkTable = rs.getString("PK_TABLE");
        String pkColumn = rs.getString("PK_COLUMN");
        int keySeq = rs.getInt("KEY_SEQ");
        String deleteRule = rs.getString("DELETE_RULE");
        String updateRule = rs.getString("UPDATE_RULE");

        // FK side
        Optional<SchemaReference> fkSchemaRef = Optional.of(new SchemaReference(Optional.empty(), schemaName));
        TableReference fkTableRef = new TableReference(fkSchemaRef, fkTable);
        ColumnReference fkColRef = new ColumnReference(Optional.of(fkTableRef), fkColumn);

        // PK side
        Optional<SchemaReference> pkSchemaRef = Optional.of(new SchemaReference(Optional.empty(), schemaName));
        TableReference pkTableRef = new TableReference(pkSchemaRef, pkTable);
        ColumnReference pkColRef = new ColumnReference(Optional.of(pkTableRef), pkColumn);

        return new ImportedKeyRecord(pkColRef, fkColRef, fkName, keySeq, mapReferentialAction(updateRule),
                mapReferentialAction(deleteRule), Optional.empty(), ImportedKey.Deferrability.NOT_DEFERRABLE);
    }

    private List<UniqueConstraint> readUniqueConstraints(Connection connection, String sql, String schema,
            String tableName) throws SQLException {
        String schemaName = resolveSchema(schema, connection);
        Map<String, UcBuilder> ucMap = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            if (tableName != null) {
                ps.setString(2, tableName);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String constraintName = rs.getString("CONSTRAINT_NAME");
                    String table = rs.getString("TABLE_NAME");
                    String columnName = rs.getString("COLUMN_NAME");

                    String key = table + "." + constraintName;
                    ucMap.computeIfAbsent(key, k -> new UcBuilder(table, constraintName, schemaName))
                            .addColumn(columnName);
                }
            }
        }
        List<UniqueConstraint> result = new ArrayList<>();
        for (UcBuilder builder : ucMap.values()) {
            result.add(builder.build());
        }
        return List.copyOf(result);
    }

    private String resolveSchema(String schema, Connection connection) throws SQLException {
        if (schema != null) {
            return schema;
        }
        // MySQL uses catalog as database name
        String catalog = connection.getCatalog();
        if (catalog != null) {
            return catalog;
        }
        return connection.getSchema() != null ? connection.getSchema() : "";
    }

    private static TriggerTiming mapTriggerTiming(String timing) {
        if (timing == null) {
            return TriggerTiming.AFTER;
        }
        return switch (timing.toUpperCase()) {
        case "BEFORE" -> TriggerTiming.BEFORE;
        default -> TriggerTiming.AFTER;
        };
    }

    private static TriggerEvent mapTriggerEvent(String event) {
        if (event == null) {
            return TriggerEvent.INSERT;
        }
        return switch (event.toUpperCase()) {
        case "UPDATE" -> TriggerEvent.UPDATE;
        case "DELETE" -> TriggerEvent.DELETE;
        default -> TriggerEvent.INSERT;
        };
    }

    private static ImportedKey.ReferentialAction mapReferentialAction(String action) {
        if (action == null) {
            return ImportedKey.ReferentialAction.NO_ACTION;
        }
        return switch (action.toUpperCase()) {
        case "CASCADE" -> ImportedKey.ReferentialAction.CASCADE;
        case "SET NULL" -> ImportedKey.ReferentialAction.SET_NULL;
        case "SET DEFAULT" -> ImportedKey.ReferentialAction.SET_DEFAULT;
        case "RESTRICT" -> ImportedKey.ReferentialAction.RESTRICT;
        default -> ImportedKey.ReferentialAction.NO_ACTION;
        };
    }

    private static IndexInfoItem.IndexType mapMySqlIndexType(String indexType) {
        if (indexType == null) {
            return IndexInfoItem.IndexType.TABLE_INDEX_OTHER;
        }
        return switch (indexType.toUpperCase()) {
        case "HASH" -> IndexInfoItem.IndexType.TABLE_INDEX_HASHED;
        case "BTREE", "FULLTEXT", "SPATIAL" -> IndexInfoItem.IndexType.TABLE_INDEX_OTHER;
        default -> IndexInfoItem.IndexType.TABLE_INDEX_OTHER;
        };
    }

    // Builder for aggregating composite primary key columns
    private static class PkBuilder {
        private final String tableName;
        private final String constraintName;
        private final String schemaName;
        private final List<String> columns = new ArrayList<>();

        PkBuilder(String tableName, String constraintName, String schemaName) {
            this.tableName = tableName;
            this.constraintName = constraintName;
            this.schemaName = schemaName;
        }

        void addColumn(String columnName) {
            columns.add(columnName);
        }

        PrimaryKey build() {
            Optional<SchemaReference> oSchema = Optional.of(new SchemaReference(Optional.empty(), schemaName));
            TableReference tableRef = new TableReference(oSchema, tableName);
            List<ColumnReference> colRefs = columns.stream()
                    .map(col -> (ColumnReference) new ColumnReference(Optional.of(tableRef), col)).toList();
            return new PrimaryKeyRecord(tableRef, colRefs, Optional.of(constraintName));
        }
    }

    // Builder for aggregating composite unique constraint columns
    private static class UcBuilder {
        private final String tableName;
        private final String constraintName;
        private final String schemaName;
        private final List<String> columns = new ArrayList<>();

        UcBuilder(String tableName, String constraintName, String schemaName) {
            this.tableName = tableName;
            this.constraintName = constraintName;
            this.schemaName = schemaName;
        }

        void addColumn(String columnName) {
            columns.add(columnName);
        }

        UniqueConstraint build() {
            Optional<SchemaReference> oSchema = Optional.of(new SchemaReference(Optional.empty(), schemaName));
            TableReference tableRef = new TableReference(oSchema, tableName);
            List<ColumnReference> colRefs = columns.stream()
                    .map(col -> (ColumnReference) new ColumnReference(Optional.of(tableRef), col)).toList();
            return new UniqueConstraintRecord(constraintName, tableRef, colRefs);
        }
    }

    // -------------------- Trigger procedures --------------------

    @Override
    public Optional<String> createTriggerProcedure(String procedureName, String schemaName, String body) {
        if (body == null || body.isBlank()) {
            throw new IllegalArgumentException("body must not be blank for MySQL trigger procedure");
        }
        String qualified = schemaName != null && !schemaName.isBlank()
                ? quoteIdentifier(schemaName, procedureName).toString()
                : quoteIdentifier(procedureName).toString();
        return Optional.of("CREATE PROCEDURE " + qualified + "() " + body);
    }

    @Override
    public String createTriggerUsingProcedure(String triggerName, String schemaName,
            org.eclipse.daanse.jdbc.db.api.schema.Trigger.TriggerTiming timing,
            org.eclipse.daanse.jdbc.db.api.schema.Trigger.TriggerEvent event,
            org.eclipse.daanse.jdbc.db.api.schema.TableReference table,
            org.eclipse.daanse.jdbc.db.api.schema.Trigger.TriggerScope scope, String whenCondition,
            String procedureName) {
        String qualified = schemaName != null && !schemaName.isBlank()
                ? quoteIdentifier(schemaName, procedureName).toString()
                : quoteIdentifier(procedureName).toString();
        return createTrigger(triggerName, timing, event, table, scope, whenCondition, "CALL " + qualified + "()");
    }

    /** MySQL/MariaDB: {@code DROP PROCEDURE [IF EXISTS] schema.procedureName}. */
    @Override
    public Optional<String> dropProcedure(String procedureName, String schemaName, boolean ifExists) {
        String qualified = schemaName != null && !schemaName.isBlank()
                ? quoteIdentifier(schemaName, procedureName).toString()
                : quoteIdentifier(procedureName).toString();
        return Optional.of("DROP PROCEDURE " + (ifExists ? "IF EXISTS " : "") + qualified);
    }

    // -------------------- DDL — ALTER (MySQL: MODIFY syntax) --------------------

    /** MySQL: {@code ALTER TABLE x MODIFY COLUMN c <type>}. */
    @Override
    public String alterColumnType(TableReference table, String columnName, ColumnMetaData newMeta) {
        if (newMeta == null) {
            throw new IllegalArgumentException("newMeta must not be null for ALTER COLUMN");
        }
        return new StringBuilder("ALTER TABLE ").append(qualified(table)).append(" MODIFY COLUMN ")
                .append(quoteIdentifier(columnName)).append(' ').append(nativeType(newMeta)).toString();
    }

    @Override
    public String alterColumnSetNullability(TableReference table, String columnName, boolean nullable) {
        throw new UnsupportedOperationException("MySQL requires the column type when altering nullability — use the "
                + "alterColumnSetNullability(table, column, nullable, currentMeta) overload.");
    }

    @Override
    public String alterColumnSetNullability(TableReference table, String columnName, boolean nullable,
            ColumnMetaData currentMeta) {
        if (currentMeta == null) {
            throw new IllegalArgumentException("currentMeta must not be null for MySQL ALTER COLUMN");
        }
        return new StringBuilder("ALTER TABLE ").append(qualified(table)).append(" MODIFY COLUMN ")
                .append(quoteIdentifier(columnName)).append(' ').append(nativeType(currentMeta))
                .append(nullable ? " NULL" : " NOT NULL").toString();
    }

    // SET DEFAULT / DROP DEFAULT inherit the SQL-99 default — MySQL 8.0+ accepts
    // it.

    /**
     * MySQL: {@code ALTER TABLE x RENAME INDEX old TO new} — index names are
     * table-scoped.
     */
    @Override
    public String renameIndex(String oldName, String newName, TableReference table) {
        if (table == null) {
            throw new IllegalArgumentException("table must not be null for MySQL RENAME INDEX");
        }
        return new StringBuilder("ALTER TABLE ").append(qualified(table)).append(" RENAME INDEX ")
                .append(quoteIdentifier(oldName)).append(" TO ").append(quoteIdentifier(newName)).toString();
    }

    /** MySQL has no constraint rename. */
    @Override
    public String renameConstraint(TableReference table, String oldName, String newName) {
        return null;
    }

    private static org.eclipse.daanse.jdbc.db.dialect.api.DialectInitData initDataFor(java.sql.Connection c) {
        try {
            return org.eclipse.daanse.jdbc.db.dialect.api.DialectInitData.fromConnection(c);
        } catch (java.sql.SQLException e) {
            return org.eclipse.daanse.jdbc.db.dialect.api.DialectInitData.ansiDefaults();
        }
    }
}
