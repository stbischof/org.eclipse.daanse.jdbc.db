/*
* Copyright (c) 2024 Contributors to the Eclipse Foundation.
*
* This program and the accompanying materials are made
* available under the terms of the Eclipse Public License 2.0
* which is available at https://www.eclipse.org/legal/epl-2.0/
*
* SPDX-License-Identifier: EPL-2.0
*
* Contributors:
*   SmartCity Jena - initial
*   Stefan Bischof (bipolis.org) - initial
*/
package org.eclipse.daanse.jdbc.db.record.schema;

import java.util.Optional;

import org.eclipse.daanse.jdbc.db.api.schema.ColumnReference;
import org.eclipse.daanse.jdbc.db.api.schema.ImportedKey;

public record ImportedKeyRecord(
        ColumnReference primaryKeyColumn,
        ColumnReference foreignKeyColumn,
        String name,
        int keySequence,
        ReferentialAction updateRule,
        ReferentialAction deleteRule,
        Optional<String> primaryKeyName,
        Deferrability deferrability) implements ImportedKey {

}
