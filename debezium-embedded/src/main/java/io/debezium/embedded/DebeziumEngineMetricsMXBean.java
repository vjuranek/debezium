/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

/**
 * Common API for Debezium engine metrics.
 *
 * @author vjuranek
 */
public interface DebeziumEngineMetricsMXBean {

    void register();

    void unregister();
}
