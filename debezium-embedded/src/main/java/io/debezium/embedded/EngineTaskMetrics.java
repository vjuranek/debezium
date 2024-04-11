/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.concurrent.atomic.AtomicLong;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * // TODO: Document this
 * @author vjuranek
 */
public class EngineTaskMetrics extends DebeziumEngineMetrics implements EngineTaskMetricsMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(EngineTaskMetrics.class);

    private final int taskId;
    private final AtomicLong totalNumberIfEventsSeen = new AtomicLong(0);
    private final AtomicLong totalNumberIfEventsProcessed = new AtomicLong(0);
    private final AtomicLong totalNumberIfEventsSeenBeforeProcessing = new AtomicLong(0);
    private final AtomicLong totalNumberIfEventsSeenAfterProcessing = new AtomicLong(0);

    public EngineTaskMetrics(int taskId) {
        this.taskId = taskId;
    }

    @Override
    public long getTotalNumberOfEventsSeen() {
        return totalNumberIfEventsSeen.get();
    }

    @Override
    public long getTotalNumberOfEventsProcessed() {
        return totalNumberIfEventsProcessed.get();
    }

    @Override
    public long getTotalNumberOfEventsSeenBeforeProcessing() {
        return totalNumberIfEventsSeenBeforeProcessing.get();
    }

    @Override
    public long getTotalNumberOfEventsSeenAfterProcessing() {
        return totalNumberIfEventsSeenAfterProcessing.get();
    }

    @Override
    public void onEventSeen() {
        totalNumberIfEventsSeen.incrementAndGet();
    }

    @Override
    public void onEventsSeen(long numberOfEventsSeen) {
        totalNumberIfEventsSeen.addAndGet(numberOfEventsSeen);
    }

    @Override
    public void onEventProcessed() {
        totalNumberIfEventsProcessed.incrementAndGet();
    }

    @Override
    public void onEventsProcessed(long numberOfEventsProcessed) {
        totalNumberIfEventsProcessed.addAndGet(numberOfEventsProcessed);
    }

    @Override
    public void onEventsSeenBeforeProcessing(long numberOfEventsSeen) {
        totalNumberIfEventsSeenBeforeProcessing.addAndGet(numberOfEventsSeen);
    }

    @Override
    public void onEventsSeenAfterProcessing(long numberOfEventsSeen) {
        totalNumberIfEventsSeenAfterProcessing.addAndGet(numberOfEventsSeen);
    }

    @Override
    protected ObjectName metricName() {
        final String metricName = "debezium.engine:type=task-metrics,taskId=task-id-" + taskId;
        try {
            return new ObjectName(metricName);
        }
        catch (MalformedObjectNameException e) {
            throw new ConnectException("Invalid metric name '" + metricName + "'");
        }
    }

    public void reset() {
        totalNumberIfEventsSeen.set(0);
        totalNumberIfEventsProcessed.set(0);
    }
}
