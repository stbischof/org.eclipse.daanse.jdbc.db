/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.daanse.jdbc.db.dialect.db.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.daanse.jdbc.db.dialect.db.testsupport.GeneratorTestSupport.table;
import static org.eclipse.daanse.jdbc.db.dialect.db.testsupport.GeneratorTestSupport.upsertSpec;

import java.util.List;
import java.util.OptionalLong;

import org.eclipse.daanse.jdbc.db.dialect.api.generator.MergeGenerator;
import org.junit.jupiter.api.Test;

/** Smoke tests for MySQL's engine-specific overrides of new generators. */
class MySqlGeneratorsTest {

    private final MySqlDialect d = new MySqlDialect();

    @Test
    void pagination_offset_limit() {
        assertThat(d.paginationGenerator().paginate(OptionalLong.of(20), OptionalLong.of(40)))
                .isEqualTo(" LIMIT 40, 20");
    }

    @Test
    void pagination_limit_only() {
        assertThat(d.paginationGenerator().paginate(OptionalLong.of(20), OptionalLong.empty())).isEqualTo(" LIMIT 20");
    }

    @Test
    void pagination_offset_only_uses_huge_limit_trick() {
        assertThat(d.paginationGenerator().paginate(OptionalLong.empty(), OptionalLong.of(40)))
                .isEqualTo(" LIMIT 40, 18446744073709551615");
    }

    @Test
    void upsert_on_duplicate_key_update() {
        MergeGenerator.UpsertSpec spec = upsertSpec(table("test", "USERS"), "ID", "NAME");
        String sql = d.mergeGenerator().upsert(spec, List.of("1", "'foo'")).orElseThrow();
        assertThat(sql).contains("INSERT INTO `test`.`USERS`")
                .contains("ON DUPLICATE KEY UPDATE `NAME` = VALUES(`NAME`)");
    }
}
