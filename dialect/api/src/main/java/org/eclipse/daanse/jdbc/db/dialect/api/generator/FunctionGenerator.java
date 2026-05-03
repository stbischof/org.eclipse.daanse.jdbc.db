/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.daanse.jdbc.db.dialect.api.generator;

public interface FunctionGenerator {

    default StringBuilder wrapIntoSqlUpperCaseFunction(CharSequence sqlExpression) {
        return new StringBuilder("UPPER(").append(sqlExpression).append(")");
    }

    /**
     * @param thenExpression value when condition is true
     * @param elseExpression value when condition is false
     */
    default StringBuilder wrapIntoSqlIfThenElseFunction(CharSequence condition, CharSequence thenExpression,
            CharSequence elseExpression) {
        return new StringBuilder("CASE WHEN ").append(condition).append(" THEN ").append(thenExpression)
                .append(" ELSE ").append(elseExpression).append(" END");
    }

    default StringBuilder generateCountExpression(CharSequence exp) {
        return new StringBuilder(exp);
    }
}
