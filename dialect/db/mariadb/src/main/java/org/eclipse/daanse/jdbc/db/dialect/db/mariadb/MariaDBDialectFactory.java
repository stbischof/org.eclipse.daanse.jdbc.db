/*
 * Copyright (c) 2022 Contributors to the Eclipse Foundation.
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
package org.eclipse.daanse.jdbc.db.dialect.db.mariadb;

import java.util.function.Function;

import org.eclipse.daanse.jdbc.db.api.meta.MetaInfo;
import org.eclipse.daanse.jdbc.db.dialect.api.DialectFactory;
import org.eclipse.daanse.jdbc.db.dialect.db.common.AbstractDialectFactory;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ServiceScope;

import aQute.bnd.annotation.spi.ServiceProvider;

@ServiceProvider(value = DialectFactory.class, attribute = { "database.dialect.type:String='MARIADB'",
    "database.product:String='MARIADB'" })
@Component(service = DialectFactory.class, scope = ServiceScope.PROTOTYPE)
public class MariaDBDialectFactory extends AbstractDialectFactory<MariaDBDialect> {
    private static final String SUPPORTED_PRODUCT_NAME = "MARIADB";

    @Override
    public boolean isSupportedProduct(String productName, String productVersion, MetaInfo metaInfo) {
        return SUPPORTED_PRODUCT_NAME.equalsIgnoreCase(productVersion);
    }

    @Override
    public Function<MetaInfo, MariaDBDialect> getConstructorFunction() {
        return MariaDBDialect::new;
    }

}