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
package org.eclipse.daanse.jdbc.db.core;

import java.util.Optional;

import org.eclipse.daanse.jdbc.db.api.SqlStatementGenerator;
import org.eclipse.daanse.jdbc.db.api.meta.MetaInfo;
import org.eclipse.daanse.jdbc.db.api.meta.TypeInfo;
import org.eclipse.daanse.jdbc.db.api.schema.ColumnDefinition;
import org.eclipse.daanse.jdbc.db.api.schema.ColumnMetaData;
import org.eclipse.daanse.jdbc.db.api.schema.ColumnReference;
import org.eclipse.daanse.jdbc.db.api.schema.ImportedKey;
import org.eclipse.daanse.jdbc.db.api.schema.Named;
import org.eclipse.daanse.jdbc.db.api.schema.TableDefinition;
import org.eclipse.daanse.jdbc.db.api.schema.TableReference;
import org.eclipse.daanse.jdbc.db.api.sql.CreateConstraintStatement;
import org.eclipse.daanse.jdbc.db.api.sql.CreateSchemaSqlStatement;
import org.eclipse.daanse.jdbc.db.api.sql.CreateSqlStatement;
import org.eclipse.daanse.jdbc.db.api.sql.DropConstraintStatement;
import org.eclipse.daanse.jdbc.db.api.sql.DropContainerSqlStatement;
import org.eclipse.daanse.jdbc.db.api.sql.DropSchemaSqlStatement;
import org.eclipse.daanse.jdbc.db.api.sql.InsertSqlStatement;
import org.eclipse.daanse.jdbc.db.api.sql.SqlStatement;
import org.eclipse.daanse.jdbc.db.api.sql.TruncateTableSqlStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlStatementGeneratorImpl implements SqlStatementGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(Logger.class);
    public static final String NO_QUOTE_FROM_METADATA = " ";

    private final MetaInfo metaInfo;
    private final String quoteString;

    public SqlStatementGeneratorImpl(MetaInfo metaInfo) {
        this.metaInfo = metaInfo;
        quoteString = metaInfo.identifierInfo().quoteString();
    }

    @Override
    public String getSqlOfStatement(SqlStatement statement) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Input SqlStatementObject: {}", statement);
        }

//Post Java 17
//        StringBuilder sb = switch (statement) {
//        case DropContainerSqlStatement dc -> writeDropContainerSqlStatement(dc);
//        case DropSchemaSqlStatement ds -> writeDropSchemaSqlStatement(ds);
//        case CreateSchemaSqlStatement cs -> writeCreateSchemaSqlStatement(cs);
//        case TruncateTableSqlStatement ts -> writeTruncateTableSqlStatement(ts);
//        case CreateSqlStatement cc -> writeCreateSqlStatement(cc);
//        case InsertSqlStatement is -> writeInsertSqlStatement(is);
//        case CreateConstraintStatement is -> writeCreateConstraintSqlStatement(is);
//        case DropConstraintStatement is -> writeDropConstraintSqlStatement(is);
//        };

        StringBuilder sb = null;
        if (statement instanceof DropContainerSqlStatement dc) {
            sb = writeDropContainerSqlStatement(dc);
        }else if (statement instanceof DropSchemaSqlStatement ds) {
            sb = writeDropSchemaSqlStatement(ds);
        }else if (statement instanceof CreateSchemaSqlStatement cs) {
            sb = writeCreateSchemaSqlStatement(cs);
        }else if (statement instanceof TruncateTableSqlStatement ts) {
            sb = writeTruncateTableSqlStatement(ts);
        }else if (statement instanceof CreateSqlStatement cc) {
            sb = writeCreateSqlStatement(cc);
        }else if (statement instanceof InsertSqlStatement is) {
            sb = writeInsertSqlStatement(is);
        }else if (statement instanceof CreateConstraintStatement ccs) {
            sb = writeCreateConstraintSqlStatement(ccs);
        }else if (statement instanceof DropConstraintStatement dc) {
            sb = writeDropConstraintSqlStatement(dc);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Generated SqlStatement: {}", sb.toString());
        }
        return sb.toString();
    }

    private StringBuilder writeDropConstraintSqlStatement(DropConstraintStatement statement) {
        //ALTER TABLE foo DROP CONSTRAINT IF EXISTS bar;
        TableReference table = statement.dropImportedKey().table();
        StringBuilder sb = new StringBuilder(20);
        sb.append("ALTER TABLE ").append(table.name());
        sb.append(" DROP CONSTRAINT ");
        if (statement.ifExists()) {
            sb.append("IF EXISTS ");
        }
        sb.append(statement.dropImportedKey().name());
        return sb;
    }

    private StringBuilder writeCreateConstraintSqlStatement(CreateConstraintStatement statement) {
        // ALTER TABLE table1 ADD CONSTRAINT `fk_table1_id1_table2_id2` FOREIGN KEY (`id1`) REFERENCES table2(`id2`)
        // ON DELETE NO ACTION ON UPDATE NO ACTION;
        ImportedKey ik = statement.importedKey();
        StringBuilder sb = new StringBuilder(20);
        sb.append("ALTER TABLE ").append(ik.foreignKeyColumn().table().get().name());
        sb.append(" ADD CONSTRAINT  ");
        sb.append(ik.name());
        sb.append(" FOREIGN KEY (");
        sb.append(ik.foreignKeyColumn().name());
        sb.append(") REFERENCES ");
        sb.append(ik.primaryKeyColumn().table().get().name());
        sb.append("(").append(ik.primaryKeyColumn().name()).append(")");
        sb.append(" ON DELETE NO ACTION ON UPDATE NO ACTION");
        return sb;
    }

    private StringBuilder writeInsertSqlStatement(InsertSqlStatement statement) {
        TableReference table = statement.table();

        StringBuilder sb = new StringBuilder(20);
        sb.append("INSERT INTO ");

        quoteContainerReference(sb, table);

        sb.append("(");

        boolean firstColumn = true;
        for (ColumnReference column : statement.columns()) {

            if (firstColumn) {
                firstColumn = false;
            } else {
                sb.append(", ");

            }

            quoteReference(sb, column);
        }

        sb.append(") VALUES (");

        boolean firstValue = true;
        for (String value : statement.values()) {

            if (firstValue) {
                firstValue = false;
            } else {
                sb.append(", ");
            }
            sb.append(value);
        }

        sb.append(")");

        return sb;
    }

    private StringBuilder writeCreateSqlStatement(CreateSqlStatement statement) {
        TableDefinition table = statement.table();

        StringBuilder sb = new StringBuilder(20);
        sb.append("CREATE ");

        sb.append(table.table().type());
        sb.append(" ");

        if (statement.ifNotExists()) {
            sb.append("IF NOT EXISTS ");
        }

        quoteContainerReference(sb, table.table());

        sb.append("( ");

        boolean first = true;
        for (ColumnDefinition columnDefinition : statement.columnDefinitions()) {

            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            quoteReference(sb, columnDefinition.column());
            sb.append(" ");

            ColumnMetaData dataType = columnDefinition.columnMetaData();

            Optional<TypeInfo> oTypeInfo = metaInfo.typeInfos().stream().filter(t -> {
                return t.dataType() == dataType.dataType();
            }).findFirst();

            String typeName = oTypeInfo.map(TypeInfo::typeName).orElse(dataType.dataType().getName());
            sb.append(typeName);

            dataType.columnSize().ifPresent(columnSize -> {
                sb.append("(");
                sb.append(columnSize);

                dataType.decimalDigits().ifPresent(i -> {
                    sb.append(",");
                    sb.append(i);
                });

                sb.append(")");
            });

        }

        sb.append(")");

        return sb;
    }

    private StringBuilder writeTruncateTableSqlStatement(TruncateTableSqlStatement statement) {

        StringBuilder sb = new StringBuilder(30);
        sb.append("TRUNCATE TABLE ");
        quoteContainerReference(sb, statement.table());
        return sb;
    }

    private StringBuilder writeCreateSchemaSqlStatement(CreateSchemaSqlStatement statement) {

        StringBuilder sb = new StringBuilder(30);
        sb.append("CREATE SCHEMA ");

        if (statement.ifNotExists()) {
            sb.append("IF NOT EXISTS ");
        }
        quoteReference(sb, statement.schema());
        return sb;

    }

    private StringBuilder writeDropSchemaSqlStatement(DropSchemaSqlStatement statement) {

        StringBuilder sb = new StringBuilder(30);
        sb.append("DROP SCHEMA ");

        if (statement.ifExists()) {
            sb.append("IF EXISTS ");
        }
        quoteReference(sb, statement.schema());
        return sb;

    }

    private StringBuilder writeDropContainerSqlStatement(DropContainerSqlStatement statement) {
        TableReference table = statement.container();

        StringBuilder sb = new StringBuilder(20);
        sb.append("DROP ");

        sb.append(table.type());
        sb.append(" ");

        if (statement.ifExists()) {
            sb.append("IF EXISTS ");
        }

        quoteContainerReference(sb, table);

        return sb;
    }

    private void quoteContainerReference(final StringBuilder sb, final TableReference containerReference) {

        containerReference.schema().ifPresent(schema -> {
            quoteReference(sb, schema);
            sb.append(".");
        });
        quoteReference(sb, containerReference);
    }

    private void quoteReference(final StringBuilder sb, final Named reference) {
        quoteIdentifier(sb, reference.name());
    }

    private void quoteIdentifier(final StringBuilder sb, final String identifiert) {

        if ((quoteString.equals(NO_QUOTE_FROM_METADATA))
            || (identifiert.startsWith(quoteString) && identifiert.endsWith(quoteString))) {
            // no quote or already quoted
            sb.append(identifiert);
            return;
        }

        String cleanedIdentifier = identifiert.replace(quoteString, quoteString + quoteString);
        sb.append(quoteString);
        sb.append(cleanedIdentifier);
        sb.append(quoteString);
    }

}
