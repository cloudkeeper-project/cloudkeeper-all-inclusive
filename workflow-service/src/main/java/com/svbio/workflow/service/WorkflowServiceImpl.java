package com.svbio.workflow.service;

import akka.actor.ActorRef;
import akka.dispatch.Futures;
import akka.dispatch.OnComplete;
import akka.pattern.AskTimeoutException;
import akka.pattern.Patterns;
import com.svbio.cloudkeeper.model.api.CloudKeeperEnvironment;
import com.svbio.cloudkeeper.model.api.WorkflowExecution;
import com.svbio.cloudkeeper.model.api.WorkflowExecutionBuilder;
import com.svbio.cloudkeeper.model.bare.element.module.BareModule;
import com.svbio.cloudkeeper.model.bare.execution.BareOverride;
import com.svbio.cloudkeeper.model.beans.element.module.MutableModule;
import com.svbio.cloudkeeper.model.beans.execution.MutableOverride;
import com.svbio.cloudkeeper.model.immutable.element.SimpleName;
import com.svbio.cloudkeeper.model.runtime.execution.RuntimeAnnotatedExecutionTrace;
import com.svbio.cloudkeeper.model.util.ImmutableList;
import com.svbio.workflow.api.ExecuteWorkflowRequest;
import com.svbio.workflow.api.ExecutionStatus;
import com.svbio.workflow.api.ExecutionStatusList;
import com.svbio.workflow.api.UnknownExecutionIdException;
import com.svbio.workflow.api.WorkflowService;
import com.svbio.workflow.util.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.Promise;
import scala.util.Failure;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

final class WorkflowServiceImpl implements WorkflowService {
    /**
     * Timeout (in ms) before which a listener must have replied with an arbitrary {@link Object} as response.
     */
    private static final long OBSERVER_TIMEOUT_MS = 10_000;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final CloudKeeperEnvironmentFactory environmentFactory;
    private final ImmutableList<ActorRef> executionObservers;
    private final StatusKeepingService statusKeepingService;
    private final ExecutionContext executionContext;

    /**
     * Monitor for synchronized access to {@link #activeExecutionMap} and its contained elements.
     */
    private final Object mutex = new Object();

    /**
     * Mapping from execution ids to active executions.
     *
     * <p>There are a few places in this code where {@code synchronized} blocks contain more than just a single
     * {@link Map} method call. Therefore, {@link Collections#synchronizedMap(Map)} is not an equivalent alternative.
     * For instance, {@link #workflowExecutionFinished} also modifies a value contained in the map. (Unfortunately, in
     * order to use JAXB, the classes in package {@link com.svbio.cloudkeeper.model.api} are mutable POJOs. This
     * requires extra work to ensure memory consistency across threads. At some places, {@code synchronized} is used
     * also for that reason.)
     */
    private final Map<Long, ActiveExecution> activeExecutionMap = new LinkedHashMap<>();

    WorkflowServiceImpl(CloudKeeperEnvironmentFactory environmentFactory, List<ActorRef> executionObservers,
            StatusKeepingService statusKeepingService, ExecutionContext executionContext) {
        this.environmentFactory = environmentFactory;
        this.executionObservers = ImmutableList.copyOf(executionObservers);
        this.statusKeepingService = statusKeepingService;
        this.executionContext = executionContext;
    }

    private static final class ActiveExecution {
        private final WorkflowExecution workflowExecution;
        private final ExecutionStatus executionStatus;

        private ActiveExecution(WorkflowExecution workflowExecution, ExecutionStatus executionStatus) {
            this.workflowExecution = workflowExecution;
            this.executionStatus = executionStatus;
        }
    }

    @Override
    public CloudKeeperEnvironment create(String prefix, boolean cleaningRequested) {
        Objects.requireNonNull(prefix);
        return new CloudKeeperEnvironmentImpl(prefix, cleaningRequested);
    }

    private final class CloudKeeperEnvironmentImpl implements CloudKeeperEnvironment {
        private final CloudKeeperEnvironment cloudKeeperEnvironment;
        private final String prefix;
        private final boolean cleaningRequested;

        private CloudKeeperEnvironmentImpl(String prefix, boolean cleaningRequested) {
            cloudKeeperEnvironment = environmentFactory.create(prefix, cleaningRequested);
            this.prefix = prefix;
            this.cleaningRequested = cleaningRequested;
        }

        @Override
        public WorkflowExecutionBuilder newWorkflowExecutionBuilder(BareModule module) {
            return new WorkflowExecutionBuilderImpl(module);
        }

        private final class WorkflowExecutionBuilderImpl implements WorkflowExecutionBuilder {
            private final ExecuteWorkflowRequest request;
            private final WorkflowExecutionBuilder builder;

            private WorkflowExecutionBuilderImpl(BareModule module) {
                request = new ExecuteWorkflowRequest()
                    .setModule(MutableModule.copyOfModule(module))
                    .setPrefix(prefix)
                    .setCleaningRequested(cleaningRequested);
                builder = cloudKeeperEnvironment.newWorkflowExecutionBuilder(module);
            }

            @Override
            public WorkflowExecutionBuilder setBundleIdentifiers(List<URI> bundleIdentifiers) {
                builder.setBundleIdentifiers(bundleIdentifiers);
                request.setBundleIdentifiers(bundleIdentifiers);
                return this;
            }

            @Override
            public WorkflowExecutionBuilder setOverrides(List<? extends BareOverride> overrides) {
                builder.setOverrides(overrides);
                request.setOverrides(
                    overrides.stream()
                        .map(MutableOverride::copyOf)
                        .collect(Collectors.toList())
                );
                return this;
            }

            @Override
            public WorkflowExecutionBuilder setInputs(Map<SimpleName, Object> inputValues) {
                builder.setInputs(inputValues);
                return this;
            }

            @Override
            public WorkflowExecution start() {
                ExecuteWorkflowRequest copiedRequest = new ExecuteWorkflowRequest(request);

                Promise<Iterable<Object>> observerCompletionPromise = Futures.promise();

                WorkflowExecution workflowExecution = builder.start();
                workflowExecution.whenHasExecutionId(
                    (@Nullable Throwable executionIdThrowable, @Nullable Long executionId) -> {
                        if (executionIdThrowable != null) {
                            observerCompletionPromise.complete(new Failure<>(executionIdThrowable));
                        } else {
                            assert executionId != null;
                            workflowExecution.whenHasRootExecutionTrace(
                                (@Nullable Throwable throwable, @Nullable RuntimeAnnotatedExecutionTrace rootTrace) -> {
                                    if (throwable == null) {
                                        assert rootTrace != null;
                                        StartExecutionEvent startExecutionEvent
                                            = new StartExecutionEvent(executionId, rootTrace, prefix);
                                        executionObservers.forEach(
                                            observer -> observer.tell(startExecutionEvent, ActorRef.noSender())
                                        );
                                    }
                                }
                            );

                            addActiveExecution(copiedRequest, executionId, workflowExecution);
                            workflowExecution.whenExecutionFinished(
                                (@Nullable Throwable finishedThrowable, @Nullable Void ignored) -> {
                                    workflowExecutionFinished(executionId, finishedThrowable);

                                    StopExecutionEvent event = new StopExecutionEvent(executionId);
                                    // Patterns.ask completes the returned future with a AskTimeoutException if the
                                    // actor does not respond within the given timeout
                                    List<Future<Object>> observerResponseFutures = executionObservers.stream()
                                        .map(observer -> Patterns.ask(observer, event, OBSERVER_TIMEOUT_MS))
                                        .collect(Collectors.toList());
                                    observerCompletionPromise.completeWith(
                                        Futures.sequence(observerResponseFutures, executionContext)
                                    );
                                }
                            );
                        }
                    }
                );

                return new WorkflowExecutionImpl(workflowExecution, observerCompletionPromise.future());
            }

            private void addActiveExecution(ExecuteWorkflowRequest request, long executionId,
                    WorkflowExecution workflowExecution) {
                // executionStatus will be safely published through the final field ActiveExecution#executionStatus
                // Also note: No need to copy the request here (it has been copied before).
                ExecutionStatus executionStatus = new ExecutionStatus()
                    .setRequest(request)
                    .setExecutionId(executionId)
                    .setStatus(ExecutionStatus.Status.RUNNING);
                synchronized (mutex) {
                    activeExecutionMap.put(executionId, new ActiveExecution(workflowExecution, executionStatus));
                }
            }
        }
    }

    private final class WorkflowExecutionImpl implements WorkflowExecution {
        private final WorkflowExecution workflowExecution;
        private final Future<Iterable<Object>> listenerCompletionFuture;

        private WorkflowExecutionImpl(WorkflowExecution workflowExecution,
                Future<Iterable<Object>> listenerCompletionFuture) {
            this.workflowExecution = workflowExecution;
            this.listenerCompletionFuture = listenerCompletionFuture;
        }

        @Override
        public long getStartTimeMillis() {
            return workflowExecution.getStartTimeMillis();
        }

        @Override
        public boolean cancel() {
            return workflowExecution.cancel();
        }

        @Override
        public void whenHasRootExecutionTrace(
                OnActionComplete<RuntimeAnnotatedExecutionTrace> onHasRootExecutionTrace) {
            workflowExecution.whenHasRootExecutionTrace(onHasRootExecutionTrace);
        }

        @Override
        public void whenHasExecutionId(OnActionComplete<Long> onHasExecutionId) {
            workflowExecution.whenHasExecutionId(onHasExecutionId);
        }

        @Override
        public boolean isRunning() {
            return !listenerCompletionFuture.isCompleted() || workflowExecution.isRunning();
        }

        @Override
        public void whenHasOutput(String outPortName, OnActionComplete<Object> onHasOutput) {
            workflowExecution.whenHasOutput(outPortName, onHasOutput);
        }

        @Override
        public void whenHasFinishTimeMillis(OnActionComplete<Long> onHasFinishTimeMillis) {
            workflowExecution.whenHasFinishTimeMillis(onHasFinishTimeMillis);
        }

        @Override
        public void whenExecutionFinished(OnActionComplete<Void> onExecutionFinished) {
            workflowExecution.whenExecutionFinished(
                (throwable, value) -> listenerCompletionFuture.onComplete(new OnComplete<Iterable<Object>>() {
                    @Override
                    public void onComplete(@Nullable Throwable listenerCompletionFailure,
                            @Nullable Iterable<Object> success) {
                        if (listenerCompletionFailure instanceof AskTimeoutException) {
                            log.warn("Workflow execution finished while execution observers {} may still be active.",
                                executionObservers);
                        }
                        onExecutionFinished.complete(throwable, value);
                    }
                }, executionContext)
            );
        }
    }

    private static <T> List<T> emptyListIfNull(@Nullable List<T> list) {
        return list == null
            ? Collections.<T>emptyList()
            : list;
    }

    @Override
    public WorkflowExecution startExecution(ExecuteWorkflowRequest request) {
        if (request.getPrefix() == null || request.getModule() == null) {
            throw new IllegalArgumentException("Invalid request is lacking prefix or module.");
        }

        CloudKeeperEnvironment cloudKeeperEnvironment = create(
            request.getPrefix(),
            request.isCleaningRequested()
        );

        return cloudKeeperEnvironment
            .newWorkflowExecutionBuilder(request.getModule())
            .setBundleIdentifiers(emptyListIfNull(request.getBundleIdentifiers()))
            .setOverrides(emptyListIfNull(request.getOverrides()))
            .start();
    }

    @Nullable
    private ActiveExecution getActiveExecution(long executionId) {
        synchronized (mutex) {
            return activeExecutionMap.get(executionId);
        }
    }

    /**
     * Internal method called <em>asynchronously</em> when an execution finishes.
     */
    private void workflowExecutionFinished(final long executionId, @Nullable Throwable throwable) {
        final ExecutionStatus executionStatus;
        synchronized (mutex) {
            executionStatus = activeExecutionMap.get(executionId).executionStatus;

            // We don't need the "global" mutex (synchronizing on executionStatus would suffice), but no need for
            // optimization yet.
            if (throwable != null) {
                executionStatus.setStatus(ExecutionStatus.Status.FAILED);
                executionStatus.setFailureDescription(Throwables.executionTraceToString(throwable));
            } else {
                executionStatus.setStatus(ExecutionStatus.Status.SUCCESSFUL);
            }
        }

        statusKeepingService.persistExecutionStatus(executionStatus).whenComplete(
            (@Nullable ExecutionStatus ignored, @Nullable Throwable persistenceThrowable) -> {
                if (persistenceThrowable != null) {
                    log.warn(String.format("Could not persist %s.", executionStatus), persistenceThrowable);
                }

                synchronized (mutex) {
                    activeExecutionMap.remove(executionId);
                }
            }
        );
    }

    /**
     * Returns the execution status of the given execution id.
     *
     * <p>The returned {@link ExecutionStatus} instance is not backed by internal state. Workflow-execution updates are
     * not reflected in the returned instance, or vice versa.
     *
     * @param executionId execution id
     * @return a future that will be completed with the execution status on success, an
     *     {@link UnknownExecutionIdException} if the given execution id does not belong to an active workflow
     *     execution, and {@link Exception} in case of any other failure
     */
    @Override
    public CompletableFuture<ExecutionStatus> getExecutionStatus(long executionId) {
        @Nullable ActiveExecution activeExecution = getActiveExecution(executionId);
        if (activeExecution != null) {
            // The synchronization is mostly for memory consistency (to establish a happens-before relationship with any
            // object modifications in other threads)
            synchronized (mutex) {
                return CompletableFuture.completedFuture(new ExecutionStatus(activeExecution.executionStatus));
            }
        } else {
            return statusKeepingService.loadExecutionStatus(executionId);
        }
    }

    /**
     * Stops the execution with the given execution id.
     *
     * @param executionId execution id
     * @throws UnknownExecutionIdException if the given execution id does not belong to an active workflow execution
     */
    @Override
    public void stopExecutionId(long executionId) throws UnknownExecutionIdException {
        @Nullable ActiveExecution execution = getActiveExecution(executionId);
        if (execution == null) {
            throw new UnknownExecutionIdException(executionId);
        }
        execution.workflowExecution.cancel();
    }

    /**
     * Returns a list of currently active workflow executions.
     *
     * <p>The returned object is not backed by internal state. Workflow-execution updates are not reflected in the
     * returned object, or vice versa.
     *
     * @return the list of currently active workflow executions
     */
    @Override
    public ExecutionStatusList getActiveExecutions() {
        synchronized (mutex) {
            ExecutionStatusList executionStatusList = new ExecutionStatusList();
            List<ExecutionStatus> list = executionStatusList.getList();
            for (ActiveExecution activeExecution: activeExecutionMap.values()) {
                list.add(new ExecutionStatus(activeExecution.executionStatus));
            }
            return executionStatusList;
        }
    }
}
