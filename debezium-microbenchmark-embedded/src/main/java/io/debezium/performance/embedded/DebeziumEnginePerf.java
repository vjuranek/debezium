/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.embedded;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import io.debezium.config.Configuration;
import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.embedded.ConvertingEngineBuilderFactory;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.util.IoUtil;

public class DebeziumEnginePerf {

    @State(Scope.Thread)
    public static class DebeziumEngineState {

        private static final Class keyValueType = Json.class;
        // private static final Class keyValueType = Connect.class;

        private DebeziumEngine<ChangeEvent<String, String>> engine;
        private ExecutorService executors;
        private BlockingQueue<ChangeEvent<String, String>> consumedLines;

        // @Param({ "10000", "50000", "100000" })
        @Param({ "10000" })
        public int events;

        @Setup(Level.Iteration)
        public void doSetup() {
            consumedLines = new ArrayBlockingQueue<>(100);

            delete("offsets.txt");
            delete("history.txt");

            Configuration config = Configuration.create()
                    .with(SimpleSourceConnector.BATCH_COUNT, 1)
                    .with(EmbeddedEngine.ENGINE_NAME, "testing-connector")
                    .with(EmbeddedEngine.CONNECTOR_CLASS, SimpleSourceConnector.class)
                    .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, getPath("offsets.txt").toAbsolutePath())
                    .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                    .with(SimpleSourceConnector.BATCH_COUNT, events)
                    .build();

            Consumer<ChangeEvent<String, String>> recordArrivedListener = this::processRecord;
            Consumer<ChangeEvent<String, String>> consumer = new Consumer<>() {
                @Override
                public void accept(ChangeEvent<String, String> record) {
                    // System.out.println("got record " + record);
                    if (Thread.currentThread().isInterrupted()) {
                        return;
                    }
                    while (!consumedLines.offer(record)) {
                        if (Thread.currentThread().isInterrupted()) {
                            return;
                        }
                    }
                    // System.out.println("accepting record " + record);
                    // try {
                    // Thread.sleep(10);
                    // }
                    // catch (InterruptedException e) {
                    // Thread.currentThread().interrupt();
                    // }
                    recordArrivedListener.accept(record);
                }
            };

            this.engine = new ConvertingEngineBuilderFactory().builder(KeyValueChangeEventFormat.of(keyValueType, keyValueType))
                    .using(config.asProperties())
                    .notifying(consumer)
                    .using(this.getClass().getClassLoader())
                    .build();

            executors = Executors.newFixedThreadPool(1);
            executors.execute(engine);
        }

        @TearDown(Level.Iteration)
        public void doCleanup() throws IOException {
            try {
                if (engine != null) {
                    engine.close();
                }
                if (executors != null) {
                    executors.shutdownNow();
                    try {
                        while (!executors.awaitTermination(60, TimeUnit.SECONDS)) {
                            // wait
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            finally {
                engine = null;
                executors = null;
            }
        }

        private Path getPath(String relativePath) {
            return Paths.get(resolveDataDir(), relativePath).toAbsolutePath();
        }

        private void delete(String relativePath) {
            Path history = getPath(relativePath).toAbsolutePath();
            if (history != null) {
                history = history.toAbsolutePath();
                if (inTestDataDir(history)) {
                    try {
                        IoUtil.delete(history);
                    }
                    catch (IOException e) {
                        // ignored
                    }
                }
            }
        }

        private boolean inTestDataDir(Path path) {
            Path target = FileSystems.getDefault().getPath(resolveDataDir()).toAbsolutePath();
            return path.toAbsolutePath().startsWith(target);
        }

        private String resolveDataDir() {
            String value = System.getProperty("dbz.test.data.dir");
            if (value != null && (value = value.trim()).length() > 0) {
                return value;
            }

            value = System.getenv("DBZ_TEST_DATA_DIR");
            if (value != null && (value = value.trim()).length() > 0) {
                return value;
            }

            return "target/data";
        }

        private void processRecord(ChangeEvent<String, String> record) {
            try {
                consumedLines.put(record);
            }
            catch (InterruptedException e) {
                throw new RuntimeException("Failed to insert record into queue", e);
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 0)
    @Measurement(iterations = 1, time = 1)
    public void capture(DebeziumEngineState state) {
        List<ChangeEvent<String, String>> records = new ArrayList<>();
        while (records.size() < state.events) {
            List<ChangeEvent<String, String>> temp = new ArrayList<>();
            state.consumedLines.drainTo(temp);
            records.addAll(temp);
        }
    }

}
