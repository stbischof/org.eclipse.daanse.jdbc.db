/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2002-2019 Hitachi Vantara and others.
 * Copyright (C) 2021 Sergei Semenkov
 * All rights reserved.
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
 *   SmartCity Jena - initial
 *   Stefan Bischof (bipolis.org) - initial
 */

package org.eclipse.daanse.jdbc.db.dialect.api;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.eclipse.daanse.jdbc.db.api.MetadataProvider;
import org.eclipse.daanse.jdbc.db.dialect.api.capability.DialectCapabilitiesProvider;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.AggregationGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.CastGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.CteGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.DdlGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.FunctionGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.HintGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.IdentifierQuoter;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.LiteralQuoter;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.MergeGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.OrderByGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.PaginationGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.ParameterPlaceholderGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.RegexGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.ReturningGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.generator.SqlGenerator;
import org.eclipse.daanse.jdbc.db.dialect.api.type.BestFitColumnType;
import org.eclipse.daanse.jdbc.db.dialect.api.type.TypeMapper;

public interface Dialect
        extends IdentifierQuoter, LiteralQuoter, DialectCapabilitiesProvider, TypeMapper, MetadataProvider {

    /**
     * Whether double values need an explicit {@code "E0"} exponent (LucidDB quirk).
     */
    boolean needsExponent(Object value, String valueString);

    /**
     * Whether the given (type, concurrency) combination is supported on this
     * engine.
     */
    boolean supportsResultSetConcurrency(int type, int concurrency);

    /**
     * Picks the most appropriate {@link BestFitColumnType} for a result-set column.
     */
    BestFitColumnType getType(ResultSetMetaData metadata, int columnIndex) throws SQLException;

    /** @return dialect name (lowercase by convention) */
    String name();

    /** @return the engine's case-folding rule for unquoted identifiers */
    default IdentifierCaseFolding caseFolding() {
        return IdentifierCaseFolding.UPPER;
    }

    default IdentifierQuotingPolicy quotingPolicy() {
        return IdentifierQuotingPolicy.ALWAYS;
    }

    // -------------------- focused-interface accessors --------------------
    //
    // The cross-cutting interfaces ({@link IdentifierQuoter}, {@link
    // LiteralQuoter},
    // {@link DialectCapabilitiesProvider}, {@link TypeMapper}, {@link
    // MetadataProvider})
    // are inherited directly — call them on the dialect. The four "builder"
    // facets below are accessed only via getters. Concrete dialects implement
    // those interfaces and return {@code this} from each getter.

    /**
     * Inline-dataset and set-operation SQL emission ({@code generateInline},
     * {@code generateUnionAllSql}).
     */
    SqlGenerator sqlGenerator();

    /**
     * All DDL emission — both string-based primitives ({@code createSchema},
     * {@code dropTable}, {@code clearTable}, {@code dropSchema}) and
     * descriptor-based builders ({@code createTable}, {@code createIndex},
     * {@code dropConstraint}, …).
     */
    DdlGenerator ddlGenerator();

    /** ORDER BY clause + NULL ordering builders. */
    OrderByGenerator orderByGenerator();

    /** Regex match/flag emission. */
    RegexGenerator regexGenerator();

    /** Aggregate and window function builders. */
    AggregationGenerator aggregationGenerator();

    /**
     * SQL scalar-function wrappers ({@code UPPER}, {@code CASE/IIF}, count
     * adornments).
     */
    FunctionGenerator functionGenerator();

    /**
     * Dialect-specific query hint emission (e.g. {@code USE INDEX},
     * {@code WITH (NOLOCK)}).
     */
    HintGenerator hintGenerator();

    /**
     * Pagination ({@code LIMIT/OFFSET} vs. {@code FETCH NEXT … ROWS ONLY} vs.
     * {@code TOP n}).
     */
    default PaginationGenerator paginationGenerator() {
        return new PaginationGenerator() {
        };
    }

    /**
     * Prepared-statement parameter placeholders ({@code ?}, {@code $n},
     * {@code @pn}).
     */
    default ParameterPlaceholderGenerator parameterPlaceholderGenerator() {
        return new ParameterPlaceholderGenerator() {
        };
    }

    /** Common-table-expression ({@code WITH [RECURSIVE] …}) emission. */
    default CteGenerator cteGenerator() {
        return new CteGenerator() {
        };
    }

    /**
     * Upsert / merge emission ({@code MERGE INTO}, {@code ON CONFLICT},
     * {@code ON DUPLICATE KEY UPDATE}).
     */
    default MergeGenerator mergeGenerator() {
        return new MergeGenerator() {
        };
    }

    /**
     * Type-cast emission ({@code CAST(x AS T)}, {@code TRY_CAST},
     * {@code SAFE_CAST}).
     */
    default CastGenerator castGenerator() {
        return new CastGenerator() {
        };
    }

    /** {@code RETURNING}/{@code OUTPUT} clause emission for DML. */
    default ReturningGenerator returningGenerator() {
        return new ReturningGenerator() {
        };
    }

}
