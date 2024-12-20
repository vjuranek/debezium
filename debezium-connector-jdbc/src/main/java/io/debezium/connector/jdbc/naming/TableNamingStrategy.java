/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.naming.CollectionNamingStrategy;

/**
 * A pluggable strategy contract for defining how table names are resolved from kafka records.
 *
 * @author Chris Cranford
 */
@Deprecated
public interface TableNamingStrategy extends CollectionNamingStrategy {

    /**
     * Resolves the logical table name from the sink record.
     *
     * @param config sink connector configuration, should not be {@code null}
     * @param record Kafka sink record, should not be {@code null}
     * @return the resolved logical table name; if {@code null} the record should not be processed
     */
    String resolveTableName(JdbcSinkConnectorConfig config, SinkRecord record);

    /**
     * Ensure compatibility with the new interface, but it must never been called directly!
     */
    default String resolveCollectionName(DebeziumSinkRecord record, String collectionNameFormat) {
        throw new UnsupportedOperationException("This should never been called!");
    }

}
