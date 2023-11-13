/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import io.debezium.config.Field;

/**
 * // TODO: Document this
 * @author vjuranek
 */
public interface AsyncEngineConfig extends EmbeddedEngineConfig {

    int AVAILABLE_CORES = Runtime.getRuntime().availableProcessors();

    /**
     * An optional field that specifies the maximum amount of time to wait for task lifecycle operation, i.e. for starting and stopping the task.
     */
    Field TASK_MANAGEMENT_TIMEOUT_MS = Field.create("task.management.timeout.ms")
            .withDescription("Time to wait for task's lifecycle management operations (starting and stopping), given in milliseconds. "
                    + "Defaults to 5 seconds (5000 ms).")
            .withDefault(5_000L)
            .withValidation(Field::isPositiveInteger);

    /**
     * An optional field that specifies maximum time in ms to wait for submitted record to finish processing during task polling when task shut down is called.
     */
    Field POLLING_SHUTDOWN_TIMEOUT_MS = Field.create("polling.shutdown.timeout.ms")
            .withDescription("Maximum time in milliseconds to wait for processing submitted records when task shutdown is called. The default is 10 seconds (10000 ms).")
            .withDefault(10_000L)
            .withValidation(Field::isPositiveInteger);

    /**
     * An optional field that specifies the number of threads to be used for processing CDC records.
     */
    Field RECORD_PROCESSING_THREADS = Field.create("record.processing.threads")
            .withDescription("The number of threads to be used for processing CDC records. The default is number of available machine cores.")
            .withDefault(AVAILABLE_CORES)
            .withValidation(Field::isPositiveInteger);

    // TODO: CONSUME_RECORDS_ASYNC

    /**
     * The array of all exposed fields.
     */
    Field.Set ALL_FIELDS = EmbeddedEngineConfig.ALL_FIELDS.with(
            TASK_MANAGEMENT_TIMEOUT_MS,
            POLLING_SHUTDOWN_TIMEOUT_MS,
            RECORD_PROCESSING_THREADS);
}
