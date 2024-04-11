/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import javax.management.ObjectName;

import io.debezium.pipeline.JmxUtils;

/**
 * // TODO: Document this
 * @author Jiri Pechanec, vjuranek
 */
public abstract class DebeziumEngineMetrics implements DebeziumEngineMetricsMXBean {
    private volatile boolean registered = false;

    /**
     * Create a JMX metric name for the given metric.
     * @return the JMX metric name
     */
    abstract protected ObjectName metricName();

    /**
     * Registers a metrics MBean into the platform MBean server.
     */
    public synchronized void register() {

        JmxUtils.registerMXBean(metricName(), this);
        // If the old metrics MBean is present then the connector will try to unregister it
        // upon shutdown.
        registered = true;
    }

    /**
     * Unregisters a metrics MBean from the platform MBean server.
     * The method is intentionally synchronized to prevent preemption between registration and unregistration.
     */
    public synchronized void unregister() {
        if (registered) {
            JmxUtils.unregisterMXBean(metricName());
            registered = false;
        }
    }
}
