/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import io.debezium.common.annotation.Incubating;

/**
 * // TODO: Document this
 * @author vjuranek
 */
@Incubating
public interface DebeziumSourceConnector {

    /**
     * Returns the {@link DebeziumSourceConnectorContext} for this DebeziumSourceConnector.
     * @return the DebeziumSourceConnectorContext for this connector
     */
    DebeziumSourceConnectorContext context();

    /**
     * Initialize the connector with its {@link DebeziumSourceConnectorContext} context.
     * @param context {@link DebeziumSourceConnectorContext} containing references to auxiliary objects.
     */
    void initialize(DebeziumSourceConnectorContext context);
}
