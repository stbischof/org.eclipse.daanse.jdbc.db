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
package org.eclipse.daanse.jdbc.db.record.meta;

import java.util.List;

import org.eclipse.daanse.jdbc.db.api.meta.StructureInfo;
import org.eclipse.daanse.jdbc.db.api.schema.CatalogReference;
import org.eclipse.daanse.jdbc.db.api.schema.CheckConstraint;
import org.eclipse.daanse.jdbc.db.api.schema.ColumnDefinition;
import org.eclipse.daanse.jdbc.db.api.schema.Function;
import org.eclipse.daanse.jdbc.db.api.schema.ImportedKey;
import org.eclipse.daanse.jdbc.db.api.schema.MaterializedView;
import org.eclipse.daanse.jdbc.db.api.schema.Partition;
import org.eclipse.daanse.jdbc.db.api.schema.PrimaryKey;
import org.eclipse.daanse.jdbc.db.api.schema.Procedure;
import org.eclipse.daanse.jdbc.db.api.schema.SchemaReference;
import org.eclipse.daanse.jdbc.db.api.schema.Sequence;
import org.eclipse.daanse.jdbc.db.api.schema.TableDefinition;
import org.eclipse.daanse.jdbc.db.api.schema.Trigger;
import org.eclipse.daanse.jdbc.db.api.schema.UniqueConstraint;
import org.eclipse.daanse.jdbc.db.api.schema.UserDefinedType;
import org.eclipse.daanse.jdbc.db.api.schema.ViewDefinition;

public record StructureInfoRecord(
        List<CatalogReference> catalogs,
        List<SchemaReference> schemas,
        List<TableDefinition> tables,
        List<ColumnDefinition> columns,
        List<ImportedKey> importedKeys,
        List<PrimaryKey> primaryKeys,
        List<Trigger> triggers,
        List<Sequence> sequences,
        List<CheckConstraint> checkConstraints,
        List<UniqueConstraint> uniqueConstraints,
        List<UserDefinedType> userDefinedTypes,
        List<ViewDefinition> viewDefinitions,
        List<Procedure> procedures,
        List<Function> functions,
        List<MaterializedView> materializedViews,
        List<Partition> partitions) implements StructureInfo {
}
