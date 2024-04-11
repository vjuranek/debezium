/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

/**
 * // TODO: Document this
 * @author vjuranek
 */
public interface EngineTaskMetricsMXBean extends DebeziumEngineMetricsMXBean {

    long getTotalNumberOfEventsSeen();

    long getTotalNumberOfEventsProcessed();

    long getTotalNumberOfEventsSeenBeforeProcessing();

    long getTotalNumberOfEventsSeenAfterProcessing();

    void onEventSeen();

    void onEventsSeen(long numberOfEventsSeen);

    void onEventProcessed();

    void onEventsProcessed(long numberOfEventsProcessed);

    void onEventsSeenBeforeProcessing(long numberOfEventsSeen);

    void onEventsSeenAfterProcessing(long numberOfEventsSeen);
}
