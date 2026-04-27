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
package org.eclipse.daanse.jdbc.db.dialect.api.type;

import java.util.stream.Stream;

import org.eclipse.daanse.jdbc.db.dialect.api.Dialect;

/**
 * Datatype of a column.
 * <p>
 * This enum represents SQL data types and provides methods for quoting
 * values appropriately for each type.
 */
public enum Datatype {

    VARCHAR("Varchar"),
    NUMERIC("Numeric"),
    INTEGER("Integer"),
    DECIMAL("Decimal"),
    FLOAT("Float"),
    REAL("Real"),
    BIGINT("BigInt"),
    SMALLINT("SmallInt"),
    DOUBLE("Double"),
    BOOLEAN("Boolean"),
    DATE("Date"),
    TIME("Time"),
    TIMESTAMP("Timestamp");

    private final String value;

    Datatype(String value) {
        this.value = value;
    }

    /**
     * Returns the string representation of this datatype.
     *
     * @return the datatype name
     */
    public String getValue() {
        return value;
    }

    /**
     * Appends to a buffer a value of this type, in the appropriate format
     * for the given dialect.
     * <p>
     * Uses enhanced switch expression to delegate to the appropriate
     * dialect quoting method based on the datatype.
     *
     * @param buf     buffer to append to
     * @param dialect dialect for quoting rules
     * @param value   value to quote
     */
    public void quoteValue(StringBuilder buf, Dialect dialect, String value) {
        switch (this) {
            case VARCHAR -> dialect.quoteStringLiteral(buf, value);
            case NUMERIC, INTEGER, DECIMAL, FLOAT, REAL, BIGINT, SMALLINT, DOUBLE
                -> dialect.quoteNumericLiteral(buf, value);
            case BOOLEAN -> dialect.quoteBooleanLiteral(buf, value);
            case DATE -> dialect.quoteDateLiteral(buf, value);
            case TIME -> dialect.quoteTimeLiteral(buf, value);
            case TIMESTAMP -> dialect.quoteTimestampLiteral(buf, value);
        }
    }

    /**
     * Returns whether this is a numeric datatype.
     *
     * @return true if this datatype represents a numeric value
     */
    public boolean isNumeric() {
        return switch (this) {
            case NUMERIC, INTEGER, DECIMAL, FLOAT, REAL, BIGINT, SMALLINT, DOUBLE -> true;
            case VARCHAR, BOOLEAN, DATE, TIME, TIMESTAMP -> false;
        };
    }

    /**
     * Returns the Datatype corresponding to the given string value.
     * <p>
     * Recognises both the enum's own labels ({@code "Varchar"}, {@code "Integer"},
     * …) and SQL-99 / JDBC spec names such as {@code "CHARACTER VARYING"},
     * {@code "DOUBLE PRECISION"}, {@code "INT"}. Match is case-insensitive and
     * collapses internal whitespace. Falls back to {@link #NUMERIC} if no match.
     */
    public static Datatype fromValue(String v) {
        if (v == null) return NUMERIC;
        String key = v.trim().toUpperCase().replaceAll("\\s+", " ");
        Datatype byAlias = SQL_NAME_ALIASES.get(key);
        if (byAlias != null) return byAlias;
        return Stream.of(Datatype.values())
            .filter(e -> e.getValue().equalsIgnoreCase(v))
            .findFirst()
            .orElse(NUMERIC);
    }

    private static final java.util.Map<String, Datatype> SQL_NAME_ALIASES = sqlNameAliases();

    private static java.util.Map<String, Datatype> sqlNameAliases() {
        java.util.Map<String, Datatype> m = new java.util.HashMap<>();
        // Character types — every flavour maps to VARCHAR.
        m.put("VARCHAR",                  VARCHAR);
        m.put("CHAR",                     VARCHAR);
        m.put("CHARACTER",                VARCHAR);
        m.put("CHARACTER VARYING",        VARCHAR);
        m.put("CHARACTER LARGE OBJECT",   VARCHAR);
        m.put("LONGVARCHAR",              VARCHAR);
        m.put("NVARCHAR",                 VARCHAR);
        m.put("NCHAR",                    VARCHAR);
        m.put("NATIONAL CHARACTER",       VARCHAR);
        m.put("NATIONAL CHARACTER VARYING", VARCHAR);
        m.put("CLOB",                     VARCHAR);
        m.put("NCLOB",                    VARCHAR);
        // Integral.
        m.put("INT",                      INTEGER);
        m.put("INTEGER",                  INTEGER);
        m.put("BIGINT",                   BIGINT);
        m.put("SMALLINT",                 SMALLINT);
        m.put("TINYINT",                  SMALLINT);
        // Numeric (non-integral).
        m.put("NUMERIC",                  NUMERIC);
        m.put("DECIMAL",                  DECIMAL);
        m.put("FLOAT",                    FLOAT);
        m.put("REAL",                     REAL);
        m.put("DOUBLE",                   DOUBLE);
        m.put("DOUBLE PRECISION",         DOUBLE);
        // Boolean / bit.
        m.put("BOOLEAN",                  BOOLEAN);
        m.put("BIT",                      BOOLEAN);
        // Temporal.
        m.put("DATE",                     DATE);
        m.put("TIME",                     TIME);
        m.put("TIME WITH TIMEZONE",       TIME);
        m.put("TIMESTAMP",                TIMESTAMP);
        m.put("TIMESTAMP WITH TIMEZONE",  TIMESTAMP);
        return m;
    }

    /**
     * Returns the Datatype corresponding to the given string value, or null if not found.
     *
     * @param v the string value to parse
     * @return the corresponding Datatype, or null if not found
     */
    public static Datatype fromValueOrNull(String v) {
        return Stream.of(Datatype.values())
            .filter(e -> e.getValue().equals(v))
            .findFirst()
            .orElse(null);
    }
}
