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
public interface DebeziumSourceTask {
    /**
     * Returns the {@link DebeziumSourceTaskContext} for this DebeziumSourceTask.
     * @return the DebeziumSourceTaskContext for this task
     */
    DebeziumSourceTaskContext context();
}
