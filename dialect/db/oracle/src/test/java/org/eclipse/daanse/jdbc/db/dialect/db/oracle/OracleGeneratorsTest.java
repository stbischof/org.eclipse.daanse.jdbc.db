/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.daanse.jdbc.db.dialect.db.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.daanse.jdbc.db.dialect.db.testsupport.GeneratorTestSupport.table;
import static org.eclipse.daanse.jdbc.db.dialect.db.testsupport.GeneratorTestSupport.upsertSpec;

import java.util.List;

import org.eclipse.daanse.jdbc.db.dialect.api.generator.MergeGenerator;
import org.junit.jupiter.api.Test;

/** Smoke tests for Oracle's engine-specific overrides of new generators. */
class OracleGeneratorsTest {

    private final OracleDialect d = new OracleDialect();

    @Test
    void merge_into_using_dual() {
        MergeGenerator.UpsertSpec spec = upsertSpec(table("RT", "USERS"), "ID", "NAME");
        String sql = d.mergeGenerator().upsert(spec, List.of("1", "'foo'")).orElseThrow();
        assertThat(sql)
                .contains("MERGE INTO \"RT\".\"USERS\" T USING (SELECT 1 AS \"ID\", 'foo' AS \"NAME\" FROM dual) S")
                .contains("ON (T.\"ID\" = S.\"ID\")").contains("WHEN MATCHED THEN UPDATE SET T.\"NAME\" = S.\"NAME\"")
                .contains("WHEN NOT MATCHED THEN INSERT");
    }
}
