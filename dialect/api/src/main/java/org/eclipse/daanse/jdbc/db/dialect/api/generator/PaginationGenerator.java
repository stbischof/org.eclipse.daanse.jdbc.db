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

import java.util.Optional;
import java.util.OptionalLong;

public interface PaginationGenerator {

    /**
     * @param limit  maximum rows to return (must be ≥ 0 if present)
     * @param offset rows to skip (must be ≥ 0 if present)
     * @return suffix to append after {@code ORDER BY …} (may be empty, never null)
     */
    default String paginate(OptionalLong limit, OptionalLong offset) {
        StringBuilder sb = new StringBuilder();
        offset.ifPresent(o -> {
            if (o < 0)
                throw new IllegalArgumentException("offset must be >= 0");
            sb.append(" OFFSET ").append(o).append(" ROWS");
        });
        limit.ifPresent(l -> {
            if (l < 0)
                throw new IllegalArgumentException("limit must be >= 0");
            sb.append(" FETCH NEXT ").append(l).append(" ROWS ONLY");
        });
        return sb.toString();
    }

    default boolean supportsOffset() {
        return true;
    }

    /** Convenience for callers that always want to paginate. */
    default Optional<String> paginate(long limit) {
        return Optional.of(paginate(OptionalLong.of(limit), OptionalLong.empty()));
    }
}
