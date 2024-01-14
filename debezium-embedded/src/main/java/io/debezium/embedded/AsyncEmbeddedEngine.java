/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.AbstractHerder;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Instantiator;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.StopEngineException;
import io.debezium.engine.source.DebeziumSourceConnector;
import io.debezium.engine.source.DebeziumSourceConnectorContext;
import io.debezium.engine.source.DebeziumSourceTask;
import io.debezium.engine.source.DebeziumSourceTaskContext;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.util.DelayStrategy;

/**
 * // TODO: Document this
 * @author vjuranek
 */
public final class AsyncEmbeddedEngine<R> implements DebeziumEngine<R>, AsyncEngineConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncEmbeddedEngine.class);

    private final Configuration config;
    private final io.debezium.util.Clock clock;
    private final ClassLoader classLoader;
    private final Consumer<SourceRecord> consumer;
    private final DebeziumEngine.ChangeConsumer<SourceRecord> handler;
    private final DebeziumEngine.CompletionCallback completionCallback;
    private final Optional<DebeziumEngine.ConnectorCallback> connectorCallback;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final WorkerConfig workerConfig;
    private final OffsetCommitPolicy offsetCommitPolicy;
    private final EngineSourceConnector connector;
    private final Transformations transformations;
    private final Function<SourceRecord, R> serializer;

    private final AtomicReference<State> state = new AtomicReference<>(State.STARTING); // state must be changed only via setEngineState() method
    private final List<EngineSourceTask> tasks = new ArrayList<>();
    private final ExecutorService taskService;
    private final ExecutorService recordService;

    private AsyncEmbeddedEngine(Properties config,
                                Consumer<SourceRecord> consumer,
                                DebeziumEngine.ChangeConsumer<SourceRecord> handler,
                                ClassLoader classLoader,
                                io.debezium.util.Clock clock,
                                DebeziumEngine.CompletionCallback completionCallback,
                                DebeziumEngine.ConnectorCallback connectorCallback,
                                OffsetCommitPolicy offsetCommitPolicy,
                                Function<SourceRecord, R> serializer) {

        this.config = Configuration.from(Objects.requireNonNull(config, "A connector configuration must be specified."));
        this.consumer = consumer;
        this.handler = Objects.requireNonNull(handler, "A connector consumer or changeHandler must be specified.");
        // this.handler = handler;
        this.classLoader = classLoader == null ? Instantiator.getClassLoader() : classLoader;
        this.clock = clock == null ? io.debezium.util.Clock.system() : clock;
        this.completionCallback = completionCallback != null ? completionCallback : new DefaultCompletionCallback();
        this.connectorCallback = Optional.ofNullable(connectorCallback);
        this.serializer = serializer;

        // Create thread pools for executing tasks and record pipelines.
        // TODO replace ConnectorConfig.TASKS_MAX_CONFIG with dedicated Engine option?
        taskService = Executors.newFixedThreadPool(this.config.getInteger(ConnectorConfig.TASKS_MAX_CONFIG, () -> 1));
        recordService = Executors.newFixedThreadPool(this.config.getInteger(AsyncEmbeddedEngine.RECORD_PROCESSING_THREADS));

        // Validate provided config and prepare Kafka worker config needed for Kafka stuff, like e.g. OffsetStore.
        if (!this.config.validateAndRecord(AsyncEngineConfig.CONNECTOR_FIELDS, LOGGER::error)) {
            DebeziumException e = new DebeziumException("Failed to start connector with invalid configuration (see logs for actual errors)", null);
            this.completionCallback.handle(false, "Failed to start connector with invalid configuration (see logs for actual errors)", e);
            throw e;
        }
        workerConfig = new EmbeddedWorkerConfig(this.config.asMap(AsyncEngineConfig.ALL_FIELDS));

        // Instantiate required objects.
        try {
            this.offsetCommitPolicy = offsetCommitPolicy == null
                    ? Instantiator.getInstanceWithProperties(this.config.getString(AsyncEngineConfig.OFFSET_COMMIT_POLICY), config)
                    : offsetCommitPolicy;
            keyConverter = Instantiator.getInstance(JsonConverter.class.getName());
            valueConverter = Instantiator.getInstance(JsonConverter.class.getName());
            transformations = new Transformations(Configuration.from(config));

            final Class<? extends SourceConnector> connectorClass = (Class<SourceConnector>) this.classLoader
                    .loadClass(this.config.getString(AsyncEngineConfig.CONNECTOR_CLASS));
            final SourceConnector connectConnector = connectorClass.getDeclaredConstructor().newInstance();
            this.connector = new EngineSourceConnector(connectConnector);
        }
        catch (Throwable t) {
            this.completionCallback.handle(false, "Failed to instantiate required class", t);
            throw new DebeziumException(t);
        }

        // Disable schema for default JSON converters.
        Map<String, String> internalConverterConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        keyConverter.configure(internalConverterConfig, true);
        valueConverter.configure(internalConverterConfig, false);
    }

    @Override
    public void run() {
        Throwable exitError = null;
        try {
            LOGGER.debug("Initializing connector and starting it");
            setEngineState(State.STARTING, State.INITIALIZING);
            connector.connectConnector().start(initializeConnector());
            LOGGER.debug("Calling connector callback after connector start");
            connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::connectorStarted);

            LOGGER.debug("Creating source tasks");
            setEngineState(State.INITIALIZING, State.CREATING_TASKS);
            createSourceTasks(connector, tasks);

            LOGGER.debug("Starting source tasks.");
            setEngineState(State.CREATING_TASKS, State.STARING_TASKS);
            startSourceTasks(tasks);

            LOGGER.debug("Starting tasks polling.");
            setEngineState(State.STARING_TASKS, State.POLLING_TASKS);
            runTasksPolling(tasks, handler);
            // Continues with infinite polling loop until close() is called or exception is thrown.
        }
        catch (Throwable t) {
            exitError = t;
            LOGGER.error("Engine has failed with ", exitError);

            final State stateBeforeStop = getEngineState();
            LOGGER.debug("Stopping " + AsyncEmbeddedEngine.class.getName());
            setEngineState(stateBeforeStop, State.STOPPING);
            try {
                if (State.STOPPING.compareTo(getEngineState()) > 0) {
                    close(stateBeforeStop);
                }
            }
            catch (Throwable ct) {
                LOGGER.error("Failed to close the engine: ", ct);
            }
        }
        finally {
            LOGGER.debug("Engine stopped");
            setEngineState(State.STOPPING, State.STOPPED);
            LOGGER.debug("Calling completion handler.");
            callCompletionHandler(exitError);
        }
    }

    @Override
    public void close() throws IOException {
        LOGGER.debug("Engine shutdown called.");
        final State engineState = getEngineState();

        // Stopping the engine is allowed from any state but State.STARING_TASKS as we typically open connections during this phase and shutdown in another thread
        // may result in leaked connections and/or other unwanted side effects.
        // If the state is State.STARTING_CONNECTOR and the state has changed to State.STARING_TASKS, we fail in setEngineState() right after this check.
        // Vice versa, if the state is State.STARTING_CONNECTOR, and we succeeded with setting the state to State.STOPPING, we eventually fail to set state before
        // calling startSourceTasks() as the state is not State.STARTING_CONNECTOR any more.
        // See https://issues.redhat.com/browse/DBZ-2534 for more details.
        if (engineState == State.STARING_TASKS) {
            throw new IllegalStateException("Cannot stop engine while tasks are starting, this may lead to leaked resource. Wait for the tasks to be fully started.");
        }

        // Stopping the tasks should be idempotent, but who knows, better to avoid situation when stop is called multiple times.
        if (engineState == State.STOPPING) {
            throw new IllegalStateException("Engine is already being shutting down.");
        }

        // Stopping already stopped engine very likely signals an error in the code using Debezium engine.
        if (engineState == State.STOPPED) {
            throw new IllegalStateException("Engine has been already shut down.");
        }

        LOGGER.debug("Stopping " + AsyncEmbeddedEngine.class.getName());
        // We can arrive to this point from multiple states, so we cannot check the expected state here.
        setEngineState(engineState, State.STOPPING);
        close(engineState);
    }

    private void close(final State stateBeforeStop) {
        stopConnector(tasks, stateBeforeStop);
    }

    /**
     * Initialize all the required pieces for initialization of the connector and returns configuration of the connector.
     */
    private Map<String, String> initializeConnector() throws Exception {
        LOGGER.debug("Preparing connector initialization");
        final String engineName = config.getString(AsyncEngineConfig.ENGINE_NAME);
        final String connectorClassName = config.getString(AsyncEngineConfig.CONNECTOR_CLASS);
        final Map<String, String> connectorConfig = validateAndGetConnectorConfig(connector.connectConnector(), connectorClassName);

        LOGGER.debug("Initializing offset store, offset reader and writer");
        final OffsetBackingStore offsetStore = createAndStartOffsetStore(connectorConfig);
        final OffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetStore, engineName, keyConverter, valueConverter);
        final OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetStore, engineName, keyConverter, valueConverter);

        LOGGER.debug("Initializing Connect connector itself");
        connector.initialize(new EngineSourceConnectorContext(this, offsetReader, offsetWriter));

        return connectorConfig;
    }

    /**
     * Creates list of connector tasks to be started as the sources of records.
     *
     * @param connector {@link EngineSourceConnector} to which the source tasks belong to.
     */
    private List<EngineSourceTask> createSourceTasks(final EngineSourceConnector connector, final List<EngineSourceTask> tasks)
            throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        final Class<? extends Task> taskClass = connector.connectConnector().taskClass();
        final List<Map<String, String>> taskConfigs = connector.connectConnector().taskConfigs(config.getInteger(ConnectorConfig.TASKS_MAX_CONFIG, 1));
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Following task configurations will be used for creating tasks:");
            for (int i = 0; i < taskConfigs.size(); i++) {
                LOGGER.debug("Config #{}: {}", i, taskConfigs.get(i));
            }
        }

        if (taskConfigs.size() < 1) {
            LOGGER.warn("No task configuration provided.");
        }

        LOGGER.debug("Creating {} instance(s) of source task(s)", taskConfigs.size());
        for (Map<String, String> taskConfig : taskConfigs) {
            final SourceTask task = (SourceTask) taskClass.getDeclaredConstructor().newInstance();
            final EngineSourceTaskContext taskContext = new EngineSourceTaskContext(
                    taskConfig,
                    connector.context().offsetStorageReader(),
                    connector.context().offsetStorageWriter(),
                    offsetCommitPolicy,
                    clock,
                    transformations);
            task.initialize(taskContext); // Initialize Kafka Connect source task
            tasks.add(new EngineSourceTask(task, taskContext)); // Create new DebeziumSourceTask
        }

        return tasks;
    }

    /**
     * Starts the source tasks.
     * The caller is responsible for handling possible error states.
     * However, all the tasks are awaited to either start of fail.
     *
     * @param tasks List of tasks to be started
     */
    private void startSourceTasks(final List<EngineSourceTask> tasks) throws Exception {
        LOGGER.debug("Starting source connector tasks.");
        final ExecutorCompletionService<Future<Void>> taskCompletionService = new ExecutorCompletionService(taskService);
        for (EngineSourceTask task : tasks) {
            taskCompletionService.submit(() -> {
                task.connectTask().start(task.context().config());
                return null;
            });
        }

        final long taskStartupTimeout = config.getLong(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS);
        LOGGER.debug("Waiting max. for {} ms for individual source tasks to start.", taskStartupTimeout);
        final int nTasks = tasks.size();
        Exception error = null;
        // To avoid leaked resources, we have to ensure that all tasks that were scheduled to start are really started before we continue with the execution in
        // the main (engine) thread and change engine state. If any of the scheduled tasks has failed, catch the exception, wait for other tasks to start and then
        // re-throw the exception and let engine stop already running tasks gracefully during the engine shutdown.
        for (int i = 0; i < nTasks; i++) {
            try {
                taskCompletionService.poll(taskStartupTimeout, TimeUnit.MILLISECONDS).get(); // we need to retrieve the results to propagate eventual exceptions
                LOGGER.debug("Started task #{} out of {} tasks.", i + 1, nTasks);
            }
            catch (Exception e) {
                LOGGER.debug("Task #{} (out of {} tasks) failed to start. Failed with {}", i + 1, nTasks, e.getMessage());
                if (LOGGER.isDebugEnabled()) {
                    e.printStackTrace();
                }

                // Store only the first error.
                if (error == null) {
                    error = e;
                }
                continue;
            }
            LOGGER.debug("Calling connector callback after task is started");
            connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::taskStarted);
        }

        // If at least one task failed to start, re-throw exception and abort the start of the connector.
        if (error != null) {
            throw error;
        }
    }

    /**
     * Schedules polling of provided tasks and wait until all polling tasks eventually finish.
     *
     * @param tasks {@link List} of {@link EngineSourceTask} tasks which should poll for the records
     * @param handler {@link DebeziumEngine.ChangeConsumer} which would consume obtained records
     */
    private void runTasksPolling(final List<EngineSourceTask> tasks, DebeziumEngine.ChangeConsumer<SourceRecord> handler)
            throws ExecutionException {
        LOGGER.debug("Starting tasks polling.");
        final ExecutorCompletionService<Void> taskCompletionService = new ExecutorCompletionService(taskService);
        for (EngineSourceTask task : tasks) {
            final RecordProcessor processor;
            // TODO
            // final RecordProcessor processor = selectRecordProcessor();
            processor = new ParallelSmtBatchProcessor(handler);
            processor.initialize(recordService, transformations, null, new SourceRecordCommitter(task));
            taskCompletionService.submit(new PollRecords(task, processor, state));
        }

        for (int i = 0; i < tasks.size(); i++) {
            try {
                taskCompletionService.take().get();
            }
            catch (InterruptedException e) {
                LOGGER.debug("Task interrupted while polling.");
                Thread.currentThread().interrupt();
            }
            LOGGER.debug("Task #{} out of {} tasks has stopped polling.", i, tasks.size());
        }
    }

    /**
     * Stops all connector's tasks. There is no checks if the tasks were fully stated or already running, stop is always called.
     * Also tries to stop all the other tasks which may be still running or awaiting execution in the task's thread pool.
     *
     * @param tasks {@link List} of {@link EngineSourceTask}s which should be stopped.
     */
    private void stopSourceTasks(final List<EngineSourceTask> tasks) {
        try {
            LOGGER.debug("Stopping source connector tasks.");
            final ExecutorCompletionService<Future<Void>> taskCompletionService = new ExecutorCompletionService(taskService);
            // TODO interrupt polling and maybe wait for polling by exposing taskCompletionService from polling method?
            // recordService.shutdown();
            // recordService.awaitTermination(TASK_POLLING_TIMEOUT_MS);
            for (EngineSourceTask task : tasks) {
                final long commitTimeout = Configuration.from(task.context().config()).getLong(EmbeddedEngineConfig.OFFSET_COMMIT_TIMEOUT_MS);
                taskCompletionService.submit(() -> {
                    LOGGER.debug("Committing task's offset.");
                    commitOffsets(task.context().offsetStorageWriter(), task.context().clock(), commitTimeout, task.connectTask());
                    LOGGER.debug("Stopping Connect task.");
                    task.connectTask().stop();
                    return null;
                });
            }

            final long taskStopTimeout = config.getLong(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS);
            LOGGER.debug("Waiting max. for {} ms for individual source tasks to stop.", taskStopTimeout);
            final int nTasks = tasks.size();
            for (int i = 0; i < nTasks; i++) {
                taskCompletionService.poll(taskStopTimeout, TimeUnit.MILLISECONDS).get(); // we need to retrieve the results to propagate eventual exceptions
                LOGGER.debug("Stopped task #{} out of {} tasks.", i + 1, nTasks);
                LOGGER.debug("Calling connector callback after task is stopped");
                connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::taskStopped);
            }

            // Some threads can still run start or poll tasks.
            LOGGER.debug("Stopping all remaining tasks if there are any.");
            taskService.awaitTermination(taskStopTimeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            LOGGER.warn("Stopping of the tasks was interrupted, shutting down immediately.");
            // TODO is it needed?
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            LOGGER.warn("Failure during stopping tasks, stopping them immediately. Failed with ", e);
            if (LOGGER.isDebugEnabled()) {
                e.printStackTrace();
            }
        }
        finally {
            // Make sure task service is shut down and no other tasks can be run.
            taskService.shutdownNow();
        }
    }

    /**
     * Stops connector's tasks.
     *
     * @param tasks {@link EngineSourceTask}s which were created by this connector and should be stopped now.
     */
    private void stopConnector(List<EngineSourceTask> tasks, State engineState) {
        if (State.STARING_TASKS.compareTo(engineState) <= 0) {
            LOGGER.debug("Stopping record pipeline executor service");
            // TODO: stop record executor service
            LOGGER.debug("Stopping the tasks.");
            stopSourceTasks(tasks);
        }
        LOGGER.debug("Stopping the connector.");
        connector.connectConnector.stop();
        LOGGER.debug("Calling connector callback after connector stop");
        connectorCallback.ifPresent(DebeziumEngine.ConnectorCallback::connectorStopped);
    }

    /**
     * Calls provided implementation of {@link DebeziumEngine.CompletionCallback}.
     *
     * @param error Error with which the engine has failed, {@code null} if the engine has finished successfully.
     */
    private void callCompletionHandler(Throwable error) {
        if (error == null) {
            completionCallback.handle(
                    true, String.format("Connector '%s' completed normally.", config.getString(AsyncEngineConfig.CONNECTOR_CLASS)), null);
        }
        else {
            if (LOGGER.isDebugEnabled()) {
                error.printStackTrace();
            }
            completionCallback.handle(false, error.getMessage(), error);
        }
    }

    /**
     * Gets the current state of the engine.
     *
     * @return current {@link State} of the {@link AsyncEmbeddedEngine}
     */
    private State getEngineState() {
        return state.get();
    }

    /**
     * Sets the new state of {@link AsyncEmbeddedEngine}.
     * Initial state is always {@code State.STARTING}.
     * State transition checks current engine state and if it doesn't correspond with actual state an exception is thrown as there is likely a bug in engine
     * implementation or usage.
     *
     * @param expectedState expected current {@link State} of the {@link AsyncEmbeddedEngine}
     * @param requestedState new {@link State} of the {@link AsyncEmbeddedEngine} to be set
     */
    private void setEngineState(State expectedState, State requestedState) {
        if (!state.compareAndSet(expectedState, requestedState)) {
            throw new IllegalStateException(
                    String.format("Cannot change engine state to '%s' as the engine is not in expected state '%s', current engine state is '%s'",
                            requestedState, expectedState, state.get()));
        }
        LOGGER.info("Engine state has changed from '{}' to '{}'", expectedState, requestedState);
    }

    /**
     * Validates provided configuration of the Kafka Connect connector and return its configuration is it's valid config.
     *
     * @param connector Kafka Connect {@link SourceConnector}
     * @param connectorClassName Class name of Kafka Connect {@link SourceConnector}
     * @return Map with connector configuration
     */
    private Map<String, String> validateAndGetConnectorConfig(final SourceConnector connector, final String connectorClassName) {
        LOGGER.debug("Validating provided connector configuration");
        final Map<String, String> connectorConfig = workerConfig.originalsStrings();
        final Config validatedConnectorConfig = connector.validate(connectorConfig);
        final ConfigInfos configInfos = AbstractHerder.generateResult(connectorClassName, Collections.emptyMap(), validatedConnectorConfig.configValues(),
                connector.config().groups());
        if (configInfos.errorCount() > 0) {
            String errors = configInfos.values().stream()
                    .flatMap(v -> v.configValue().errors().stream())
                    .collect(Collectors.joining(" "));
            throw new DebeziumException("Connector configuration is not valid. " + errors);
        }
        LOGGER.debug("Connector configuration is valid");
        return connectorConfig;
    }

    /**
     * Determines, which offset backing store should be used, instantiate it and start the offset store.
     */
    private OffsetBackingStore createAndStartOffsetStore(final Map<String, String> connectorConfig) throws Exception {
        final String offsetStoreClassName = config.getString(AsyncEngineConfig.OFFSET_STORAGE);

        LOGGER.debug("Creating instance of offset store for {}", offsetStoreClassName);
        final OffsetBackingStore offsetStore;
        // Kafka 3.5 no longer provides offset stores with non-parametric constructors
        if (offsetStoreClassName.equals(MemoryOffsetBackingStore.class.getName())) {
            offsetStore = KafkaConnectUtil.memoryOffsetBackingStore();
        }
        else if (offsetStoreClassName.equals(FileOffsetBackingStore.class.getName())) {
            offsetStore = KafkaConnectUtil.fileOffsetBackingStore();
        }
        else if (offsetStoreClassName.equals(KafkaOffsetBackingStore.class.getName())) {
            offsetStore = KafkaConnectUtil.kafkaOffsetBackingStore(connectorConfig);
        }
        else {
            Class<? extends OffsetBackingStore> offsetStoreClass = (Class<OffsetBackingStore>) classLoader.loadClass(offsetStoreClassName);
            offsetStore = offsetStoreClass.getDeclaredConstructor().newInstance();
        }

        try {
            LOGGER.debug("Starting offset store");
            offsetStore.configure(workerConfig);
            offsetStore.start();
        }
        catch (Throwable t) {
            LOGGER.debug("Failed to start offset store, stopping it now");
            offsetStore.stop();
            throw t;
        }

        LOGGER.debug("Offset store {} successfully started", offsetStoreClassName);
        return offsetStore;
    }

    /**
     * Commits the offset to {@link OffsetBackingStore} via {@link OffsetStorageWriter}.
     *
     * @param offsetWriter {@link OffsetStorageWriter} which performs the flushing the offset into {@link OffsetBackingStore}
     * @param commitTimeout amount of time to wait for offset flush to finish before it's aborted
     * @param task {@link SourceTask} this performs the offset commit
     * @return {@code true} if the offset was successfully committed, {@code false} otherwise
     */
    protected static boolean commitOffsets(
                                           final OffsetStorageWriter offsetWriter,
                                           final io.debezium.util.Clock clock,
                                           final long commitTimeout,
                                           final SourceTask task)
            throws InterruptedException, TimeoutException {
        long timeout = clock.currentTimeInMillis() + commitTimeout;
        if (!offsetWriter.beginFlush(commitTimeout, TimeUnit.MICROSECONDS)) {
            LOGGER.debug("No offset to be committed.");
            return false;
        }

        Future<Void> flush = offsetWriter.doFlush((Throwable error, Void result) -> {
        });
        if (flush == null) {
            LOGGER.warn("Flushing process probably failed, please check previous log for more details.");
            return false;
        }

        try {
            flush.get(Math.max(timeout - clock.currentTimeInMillis(), 0), TimeUnit.MILLISECONDS);
            task.commit();
        }
        catch (InterruptedException e) {
            LOGGER.debug("Flush of the offsets interrupted, canceling the flush.");
            offsetWriter.cancelFlush();
            throw e;
        }
        catch (ExecutionException | TimeoutException e) {
            LOGGER.warn("Flush of the offsets failed, canceling the flush.");
            offsetWriter.cancelFlush();
            return false;
        }
        return true;
    }

    /**
     * Generalization of {@link DebeziumEngine.ChangeConsumer}, giving complete control over the records processing.
     * Processor is initialized with all the required engine internals, like chain of transformations, to be able to implement whole record processing chain.
     * Implementations can provide e.g. serial or parallel processing of the change records.
     */
    @Incubating
    public interface RecordProcessor<R> {

        /**
         * Initialize the processor with objects created and managed by {@link DebeziumEngine}, which are needed for records processing.
         *
         * @param recordService {@link ExecutorService} which allows to run processing of individual records in parallel
         * @param transformations chain of transformations to be applied on every individual record
         * @param serializer converter converting {@link SourceRecord} into desired format
         * @param committer implementation of {@link DebeziumEngine.RecordCommitter} responsible for committing individual records as well as batches
         */
        void initialize(ExecutorService recordService, Transformations transformations, Function<SourceRecord, R> serializer, RecordCommitter committer);

        /**
         * Processes a batch of records provided by the source connector.
         * Implementations are assumed to use {@link DebeziumEngine.RecordCommitter} to appropriately commit individual records and the batch itself.
         *
         * @param records List of {@link SourceRecord} provided by the source connector to be processed.
         * @throws InterruptedException
         */
        void processRecords(List<SourceRecord> records) throws InterruptedException;
    }

    private abstract class AbstractRecordProcessor implements RecordProcessor<R> {
        protected ExecutorService recordService;
        protected Transformations transformations;
        protected Function<SourceRecord, R> convertor;
        protected RecordCommitter committer;

        @Override
        public void initialize(final ExecutorService recordService, final Transformations transformations, final Function<SourceRecord, R> convertor,
                               final RecordCommitter committer) {
            this.recordService = recordService;
            this.transformations = transformations;
            this.convertor = convertor;
            this.committer = committer;
        }

        @Override
        public abstract void processRecords(List<SourceRecord> records) throws InterruptedException;
    }

    /**
     * Possible engine states.
     * Engine state must be changed only via {@link AsyncEmbeddedEngine#setEngineState(State, State)} method.
     */
    private enum State {
        // Order of the possible states is important, enum ordinal is used for state comparison.
        // TODO add explicit ordinal value to avoid possible issue is the order of values is changed accidentally
        STARTING, // the engine is being started, which mostly means engine object is being created or was already created, but run() method wasn't called yet
        INITIALIZING, // initializing the connector
        CREATING_TASKS, // creating connector tasks
        STARING_TASKS, // starting connector tasks
        POLLING_TASKS, // running tasks polling, this is the main phase when the data are produced
        STOPPING, // the engine is being stopped
        STOPPED; // engine has been stopped, final state, cannot move any further from this state and any call on engine in this state should fail
    }

    /**
     * Default completion callback which just logs the error. If connector finished successfully it does nothing.
     */
    private static class DefaultCompletionCallback implements DebeziumEngine.CompletionCallback {
        @Override
        public void handle(boolean success, String message, Throwable error) {
            if (!success) {
                LOGGER.error(message, error);
            }
        }
    }

    // ========================================================= TODO ======================================================

    /**
     * {@link Callable} which in the loop polls the connector for the records.
     * If there are any records, they are passed to provided processor.
     * The {@link Callable} is {@link RetryingCallable} - if the {@link org.apache.kafka.connect.errors.RetriableException}
     * is thrown, the {@link Callable} is executed again according to configured {@link DelayStrategy} and number of retries.
     */
    private static class PollRecords extends RetryingCallable<Void> {
        final EngineSourceTask task;
        final RecordProcessor processor;
        final AtomicReference<State> engineState;

        PollRecords(final EngineSourceTask task, final RecordProcessor processor, final AtomicReference<State> engineState) {
            super(Configuration.from(task.context().config()).getInteger(EmbeddedEngineConfig.ERRORS_MAX_RETRIES));
            this.task = task;
            this.processor = processor;
            this.engineState = engineState;
        }

        @Override
        public Void doCall() throws Exception {
            while (engineState.get() == State.POLLING_TASKS) {
                LOGGER.debug("Thread {} running task {} starts polling for records.", Thread.currentThread().getName(), task.connectTask());
                final List<SourceRecord> changeRecords = task.connectTask().poll(); // blocks until there are values ...
                LOGGER.debug("Thread {} polled {} records.", Thread.currentThread().getName(), changeRecords == null ? "no" : changeRecords.size());
                if (changeRecords != null && !changeRecords.isEmpty()) {
                    processor.processRecords(changeRecords);
                }
                else {
                    LOGGER.debug("No records.");
                }
            }
            return null;
        }

        @Override
        public DelayStrategy delayStrategy() {
            final Configuration config = Configuration.from(task.context().config());
            return DelayStrategy.exponential(Duration.ofMillis(config.getInteger(EmbeddedEngineConfig.ERRORS_RETRY_DELAY_INITIAL_MS)),
                    Duration.ofMillis(config.getInteger(EmbeddedEngineConfig.ERRORS_RETRY_DELAY_MAX_MS)));
        }
    }

    /**
     * For testing purposes ONLY.
     * Exposes tasks to a use defined consumer, which allows to run the tasks in tests.
     *
     * @param consumer {@link Consumer} for running tasks.
     */
    @VisibleForTesting
    public void runWithTask(Consumer<SourceTask> consumer) {
        for (EngineSourceTask task : tasks) {
            consumer.accept(task.connectTask());
        }
    }

    private static class TransformRecord implements Callable<SourceRecord> {
        private final SourceRecord record;
        private final Transformations transformations;

        TransformRecord(final SourceRecord record, final Transformations transformations) {
            this.record = record;
            this.transformations = transformations;
        }

        @Override
        public SourceRecord call() {
            final SourceRecord transformedRecord = transformations.transform(record);
            return transformedRecord != null ? transformedRecord : null;
        }
    }

    private class TransformAndConvertRecord<R> implements Callable<R> {
        private final SourceRecord record;
        private final Transformations transformations;
        private final Function<SourceRecord, R> converter;

        TransformAndConvertRecord(final SourceRecord record, final Transformations transformations, final Function<SourceRecord, R> converter) {
            this.record = record;
            this.transformations = transformations;
            this.converter = converter;
        }

        @Override
        public R call() {
            final SourceRecord transformedRecord = transformations.transform(record);
            return transformedRecord != null ? converter.apply(transformedRecord) : null;
        }
    }

    private static class TransformConvertConsumeRecord<R> implements Callable<Void> {
        private final SourceRecord record;
        private final Transformations transformations;
        private final Function<SourceRecord, R> serializer;
        private final Consumer<R> consumer;

        TransformConvertConsumeRecord(
                                      final SourceRecord record,
                                      final Transformations transformations,
                                      final Function<SourceRecord, R> serializer,
                                      final Consumer<R> consumer) {
            this.record = record;
            this.transformations = transformations;
            this.serializer = serializer;
            this.consumer = consumer;
        }

        @Override
        public Void call() throws InterruptedException {
            final SourceRecord transformedRecord = transformations.transform(record);
            if (transformedRecord != null) {
                consumer.accept(serializer.apply(transformedRecord));
            }
            return null;
        }
    }

    private class ParallelSmtBatchProcessor extends AbstractRecordProcessor {
        final DebeziumEngine.ChangeConsumer<SourceRecord> userHandler;

        ParallelSmtBatchProcessor(final DebeziumEngine.ChangeConsumer<SourceRecord> userHandler) {
            this.userHandler = userHandler;
        }

        @Override
        public void processRecords(final List<SourceRecord> records) throws InterruptedException {
            LOGGER.debug("Thread {} is submitting {} records for processing.", Thread.currentThread().getName(), records.size());
            final List<Future<SourceRecord>> recordFutures = new ArrayList<>(records.size());

            // changeRecords.stream().forEachOrdered();
            for (SourceRecord record : records) {
                recordFutures.add(recordService.submit(new TransformRecord(record, transformations)));
            }

            LOGGER.debug("Thread {} is getting source records.", Thread.currentThread().getName());
            final List<SourceRecord> transformedRecords = new ArrayList<>(recordFutures.size());
            try {
                for (Future<SourceRecord> f : recordFutures) {
                    SourceRecord record = f.get(); // we need the whole batch, eventually wait forever
                    if (record != null) {
                        transformedRecords.add(record);
                    }
                }
            }
            catch (Exception e) {
                // TODO fix it
                LOGGER.error("failed with ", e);
            }

            userHandler.handleBatch(transformedRecords, committer);
        }
    }

    private class ParallelSmtAndConvertBatchProcessor extends AbstractRecordProcessor {
        final DebeziumEngine.ChangeConsumer<R> userHandler;

        ParallelSmtAndConvertBatchProcessor(final DebeziumEngine.ChangeConsumer<R> userHandler) {
            this.userHandler = userHandler;
        }

        @Override
        public void processRecords(final List<SourceRecord> records) throws InterruptedException {
            LOGGER.debug("Submitting {} records for processing.", records.size());
            final List<Future<R>> recordFutures = new ArrayList<>(records.size());

            // changeRecords.stream().forEachOrdered();
            for (SourceRecord record : records) {
                recordFutures.add(recordService.submit(new TransformAndConvertRecord<R>(record, transformations, convertor)));
            }

            LOGGER.debug("Getting source records.");
            final List<R> converteddRecords = new ArrayList<>(recordFutures.size());
            try {
                for (Future<R> f : recordFutures) {
                    R record = f.get(); // we need the whole batch, eventually wait forever
                    if (record != null) {
                        converteddRecords.add(record);
                    }
                }
            }
            catch (Exception e) {
                // TODO fix it
                LOGGER.error("failed with ", e);
            }

            userHandler.handleBatch(converteddRecords, committer);
        }
    }

    private class ParallelSmtConsumerProcessor extends AbstractRecordProcessor {
        final Consumer<SourceRecord> consumer;

        ParallelSmtConsumerProcessor(final Consumer<SourceRecord> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void processRecords(final List<SourceRecord> records) throws InterruptedException {
            LOGGER.debug("Submitting {} records for processing.", records.size());
            final List<Future<SourceRecord>> recordFutures = new ArrayList<>(records.size());
            for (SourceRecord record : records) {
                recordFutures.add(recordService.submit(new TransformRecord(record, transformations)));
            }

            LOGGER.debug("Waiting for the batch to finish processing.");
            final List<SourceRecord> convertedRecords = new ArrayList<>(recordFutures.size());
            try {
                for (Future<SourceRecord> f : recordFutures) {
                    convertedRecords.add(f.get()); // we need the whole batch, eventually wait forever
                }
            }
            catch (Exception e) {
                // TODO fix it
                LOGGER.error("failed with ", e);
            }

            for (int i = 0; i < records.size(); i++) {
                consumer.accept(convertedRecords.get(i));
                committer.markProcessed(records.get(i));
            }

            LOGGER.debug("Marking batch as finished.");
            committer.markBatchFinished();
        }
    }

    private class ParallelSmtAndConvertConsumerProcessor extends AbstractRecordProcessor {
        final Consumer<R> consumer;

        ParallelSmtAndConvertConsumerProcessor(final Consumer<R> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void processRecords(final List<SourceRecord> records) throws InterruptedException {
            LOGGER.debug("Submitting {} records for processing.", records.size());
            final List<Future<R>> recordFutures = new ArrayList<>(records.size());
            for (SourceRecord record : records) {
                recordFutures.add(recordService.submit(new TransformAndConvertRecord(record, transformations, serializer)));
            }

            LOGGER.debug("Waiting for the batch to finish processing.");
            final List<R> convertedRecords = new ArrayList<>(recordFutures.size());
            try {
                for (Future<R> f : recordFutures) {
                    convertedRecords.add(f.get()); // we need the whole batch, eventually wait forever
                }
            }
            catch (Exception e) {
                // TODO fix it
                LOGGER.error("failed with ", e);
            }

            for (int i = 0; i < records.size(); i++) {
                consumer.accept(convertedRecords.get(i));
                committer.markProcessed(records.get(i));
            }

            LOGGER.debug("Marking batch as finished.");
            committer.markBatchFinished();
        }
    }

    private class ParallelSmtAndConvertAsyncConsumerProcessor extends AbstractRecordProcessor {
        final Consumer<R> consumer;

        ParallelSmtAndConvertAsyncConsumerProcessor(final Consumer<R> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void processRecords(final List<SourceRecord> records) throws InterruptedException {
            LOGGER.debug("Submitting {} records for processing.", records.size());
            final List<Future<Void>> recordFutures = new ArrayList<>(records.size());
            for (SourceRecord record : records) {
                recordFutures.add(recordService.submit(new TransformConvertConsumeRecord<>(record, transformations, convertor, consumer)));
            }

            LOGGER.debug("Waiting for the batch to finish processing.");
            for (int i = 0; i < records.size(); i++) {
                recordFutures.get(i);
                committer.markProcessed(records.get(i));
            }

            LOGGER.debug("Marking batch as finished.");
            committer.markBatchFinished();
        }
    }

    /**
     * The default implementation of {@link DebeziumEngine.RecordCommitter}.
     */
    private static class SourceRecordCommitter implements DebeziumEngine.RecordCommitter<SourceRecord> {

        final SourceTask task;
        final OffsetStorageWriter offsetWriter;
        final OffsetCommitPolicy offsetCommitPolicy;
        final io.debezium.util.Clock clock;
        final long commitTimeout;

        private long recordsSinceLastCommit = 0;
        private long timeOfLastCommitMillis = 0;

        SourceRecordCommitter(final EngineSourceTask task) {
            this.task = task.connectTask();
            this.offsetWriter = task.context().offsetStorageWriter();
            this.offsetCommitPolicy = task.context().offsetCommitPolicy();
            this.clock = task.context().clock();
            this.commitTimeout = Configuration.from(task.context().config()).getLong(EmbeddedEngineConfig.OFFSET_COMMIT_TIMEOUT_MS);
        }

        @Override
        // TODO do we need synchronized here? Possibly not.
        public synchronized void markProcessed(SourceRecord record) throws InterruptedException {
            task.commitRecord(record);
            recordsSinceLastCommit += 1;
            offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
        }

        @Override
        public synchronized void markBatchFinished() throws InterruptedException {
            final Duration durationSinceLastCommit = Duration.ofMillis(clock.currentTimeInMillis() - timeOfLastCommitMillis);
            if (offsetCommitPolicy.performCommit(recordsSinceLastCommit, durationSinceLastCommit)) {
                try {
                    if (commitOffsets(offsetWriter, clock, commitTimeout, task)) {
                        recordsSinceLastCommit = 0;
                        timeOfLastCommitMillis = clock.currentTimeInMillis();
                    }
                }
                catch (TimeoutException e) {
                    throw new DebeziumException("Timed out while waiting for committing task offset", e);
                }
            }
        }

        @Override
        public synchronized void markProcessed(SourceRecord record, DebeziumEngine.Offsets sourceOffsets) throws InterruptedException {
            DebeziumEngineCommon.SourceRecordOffsets offsets = (DebeziumEngineCommon.SourceRecordOffsets) sourceOffsets;
            SourceRecord recordWithUpdatedOffsets = new SourceRecord(record.sourcePartition(), offsets.getOffsets(), record.topic(),
                    record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(),
                    record.timestamp(), record.headers());
            markProcessed(recordWithUpdatedOffsets);
        }

        @Override
        public DebeziumEngine.Offsets buildOffsets() {
            return new DebeziumEngineCommon.SourceRecordOffsets();
        }
    }

    /**
     * Implementation of {@link DebeziumEngine.Builder} which creates {@link AsyncEngineBuilder}.
     */
    public static final class AsyncEngineBuilder implements DebeziumEngine.Builder<SourceRecord> {

        private Properties config;
        private Consumer<SourceRecord> consumer;
        private DebeziumEngine.ChangeConsumer<SourceRecord> handler;
        private ClassLoader classLoader;
        private io.debezium.util.Clock clock;
        private DebeziumEngine.CompletionCallback completionCallback;
        private DebeziumEngine.ConnectorCallback connectorCallback;
        private OffsetCommitPolicy offsetCommitPolicy = null;

        @Override
        public Builder<SourceRecord> notifying(Consumer<SourceRecord> consumer) {
            this.consumer = consumer;
            this.handler = buildDefaultChangeConsumer(consumer);
            return this;
        }

        @Override
        public Builder<SourceRecord> notifying(ChangeConsumer<SourceRecord> handler) {
            this.handler = handler;
            if (!config.contains(CommonConnectorConfig.TOMBSTONES_ON_DELETE.name()) && !handler.supportsTombstoneEvents()) {
                LOGGER.info("Consumer doesn't support tombstone events, setting '{}' to false.", CommonConnectorConfig.TOMBSTONES_ON_DELETE.name());
                config.put(CommonConnectorConfig.TOMBSTONES_ON_DELETE.name(), "false");
            }
            return this;
        }

        @Override
        public Builder<SourceRecord> using(Properties config) {
            this.config = config;
            return this;
        }

        @Override
        public Builder<SourceRecord> using(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        @Override
        public Builder<SourceRecord> using(Clock clock) {
            this.clock = new io.debezium.util.Clock() {
                @Override
                public long currentTimeInMillis() {
                    return clock.millis();
                }
            };
            return this;
        }

        @Override
        public Builder<SourceRecord> using(CompletionCallback completionCallback) {
            this.completionCallback = completionCallback;
            return this;
        }

        @Override
        public Builder<SourceRecord> using(ConnectorCallback connectorCallback) {
            this.connectorCallback = connectorCallback;
            return this;
        }

        @Override
        public Builder<SourceRecord> using(OffsetCommitPolicy policy) {
            this.offsetCommitPolicy = policy;
            return this;
        }

        @Override
        public DebeziumEngine<SourceRecord> build() {
            return new AsyncEmbeddedEngine(config, consumer, handler, classLoader, clock, completionCallback, connectorCallback, offsetCommitPolicy, null);
        }
    }

    private static ChangeConsumer<SourceRecord> buildDefaultChangeConsumer(Consumer<SourceRecord> consumer) {
        return new DebeziumEngine.ChangeConsumer<SourceRecord>() {

            /**
             * the default implementation that is compatible with the old Consumer api.
             *
             * On every record, it calls the consumer, and then only marks the record
             * as processed when accept returns, additionally, it handles StopEngineException
             * and ensures that we all ways try and mark a batch as finished, even with exceptions
             * @param records the records to be processed
             * @param committer the committer that indicates to the system that we are finished
             *
             * @throws Exception
             */
            @Override
            public void handleBatch(List<SourceRecord> records, DebeziumEngine.RecordCommitter<SourceRecord> committer) throws InterruptedException {
                for (SourceRecord record : records) {
                    try {
                        consumer.accept(record);
                        committer.markProcessed(record);
                    }
                    catch (StopEngineException ex) {
                        // ensure that we mark the record as finished
                        // in this case
                        committer.markProcessed(record);
                        throw ex;
                    }
                }
                committer.markBatchFinished();
            }
        };
    }

    public static final class EngineSourceConnectorContext implements DebeziumSourceConnectorContext, SourceConnectorContext {

        private final AsyncEmbeddedEngine engine;
        private final OffsetStorageReader offsetReader;
        private final OffsetStorageWriter offsetWriter;

        public EngineSourceConnectorContext(final AsyncEmbeddedEngine engine, final OffsetStorageReader offsetReader, final OffsetStorageWriter offsetWriter) {
            this.engine = engine;
            this.offsetReader = offsetReader;
            this.offsetWriter = offsetWriter;
        }

        @Override
        public OffsetStorageReader offsetStorageReader() {
            return offsetReader;
        }

        @Override
        public OffsetStorageWriter offsetStorageWriter() {
            return offsetWriter;
        }

        @Override
        public void requestTaskReconfiguration() {
            // no-op, we don't support config changes on the fly yet
        }

        @Override
        public void raiseError(Exception e) {
            // no-op, only for Kafka compatibility
        }
    }

    public static final class EngineSourceTaskContext implements DebeziumSourceTaskContext, SourceTaskContext {

        private final Map<String, String> config;
        private final OffsetStorageReader offsetReader;
        private final OffsetStorageWriter offsetWriter;
        private final OffsetCommitPolicy offsetCommitPolicy;
        private final io.debezium.util.Clock clock;
        private final Transformations transformations;

        public EngineSourceTaskContext(
                                       final Map<String, String> config,
                                       final OffsetStorageReader offsetReader,
                                       final OffsetStorageWriter offsetWriter,
                                       final OffsetCommitPolicy offsetCommitPolicy,
                                       final io.debezium.util.Clock clock,
                                       final Transformations transformations) {
            this.config = config;
            this.offsetReader = offsetReader;
            this.offsetWriter = offsetWriter;
            this.offsetCommitPolicy = offsetCommitPolicy;
            this.clock = clock;
            this.transformations = transformations;
        }

        @Override
        public Map<String, String> config() {
            return config;
        }

        @Override
        public OffsetStorageReader offsetStorageReader() {
            return offsetReader;
        }

        @Override
        public OffsetStorageWriter offsetStorageWriter() {
            return offsetWriter;
        }

        @Override
        public OffsetCommitPolicy offsetCommitPolicy() {
            return offsetCommitPolicy;
        }

        @Override
        public io.debezium.util.Clock clock() {
            return clock;
        }

        @Override
        public Transformations transformations() {
            return transformations;
        }

        @Override
        public Map<String, String> configs() {
            // we don't support config changes on the fly yet
            return null;
        }
    }

    /**
     * Just to make context available.
     */
    public static final class EngineSourceConnector implements DebeziumSourceConnector {

        private final SourceConnector connectConnector;
        private DebeziumSourceConnectorContext context;

        public EngineSourceConnector(final SourceConnector connectConnector) {
            this.connectConnector = connectConnector;
        }

        public SourceConnector connectConnector() {
            return connectConnector;
        }

        @Override
        public DebeziumSourceConnectorContext context() {
            return this.context;
        }

        @Override
        public void initialize(DebeziumSourceConnectorContext context) {
            this.context = context;
            this.connectConnector.initialize((SourceConnectorContext) context);
        }
    }

    public static final class EngineSourceTask implements DebeziumSourceTask {

        private final SourceTask connectTask;
        private final DebeziumSourceTaskContext context;

        public EngineSourceTask(final SourceTask connectTask, final DebeziumSourceTaskContext context) {
            this.connectTask = connectTask;
            this.context = context;
        }

        @Override
        public DebeziumSourceTaskContext context() {
            return context;
        }

        public SourceTask connectTask() {
            return connectTask;
        }
    }
}
