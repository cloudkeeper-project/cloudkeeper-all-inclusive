package com.svbio.workflow.service;

import akka.actor.ActorSystem;
import akka.dispatch.ExecutionContexts;
import akka.testkit.JavaTestKit;
import akka.testkit.TestProbe;
import cloudkeeper.annotations.CloudKeeperSerialization;
import com.svbio.cloudkeeper.model.api.CancellationException;
import com.svbio.cloudkeeper.model.api.CloudKeeperEnvironment;
import com.svbio.cloudkeeper.model.api.WorkflowExecution;
import com.svbio.cloudkeeper.model.api.WorkflowExecutionBuilder;
import com.svbio.cloudkeeper.model.bare.element.module.BareModule;
import com.svbio.cloudkeeper.model.bare.execution.BareOverride;
import com.svbio.cloudkeeper.model.beans.element.annotation.MutableAnnotation;
import com.svbio.cloudkeeper.model.beans.element.annotation.MutableAnnotationEntry;
import com.svbio.cloudkeeper.model.beans.element.module.MutableModule;
import com.svbio.cloudkeeper.model.beans.element.module.MutableProxyModule;
import com.svbio.cloudkeeper.model.beans.execution.MutableExecutionTraceTarget;
import com.svbio.cloudkeeper.model.beans.execution.MutableOverride;
import com.svbio.cloudkeeper.model.immutable.element.SimpleName;
import com.svbio.cloudkeeper.model.runtime.execution.RuntimeAnnotatedExecutionTrace;
import com.svbio.workflow.api.ExecuteWorkflowRequest;
import com.svbio.workflow.api.ExecutionStatus;
import com.svbio.workflow.api.UnknownExecutionIdException;
import com.svbio.workflow.api.WorkflowService;
import com.svbio.workflow.util.Throwables;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.concurrent.ExecutionContext;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class WorkflowServiceImplTest {
    private static final List<URI> BUNDLE_IDENTIFIERS = Collections.singletonList(URI.create("x-test:foo"));
    private static final String DECLARATION = "foo.Module";
    private static final long EXECUTION_ID = 81;
    private static final String PREFIX = "prefix";
    private static final String IN_PORT = "baz";
    private static final String IN_PORT_ARG = "qux";
    private static final String OUT_PORT = "foo";
    private static final String OUT_PORT_RESULT = "bar";

    private static class TestCase implements AutoCloseable {
        private final MockCloudKeeperEnvironmentFactory factory = new MockCloudKeeperEnvironmentFactory();
        private final ActorSystem actorSystem;
        private final TestProbe eventListener;
        private final MockStatusKeepingService statusKeepingService = new MockStatusKeepingService();
        private final WorkflowServiceImpl environmentFactory;
        private final CallingThreadExecutor executor = new CallingThreadExecutor();
        private final ExecutionContext executionContext = ExecutionContexts.fromExecutor(executor);

        private TestCase(String name) {
            actorSystem = ActorSystem.create(name);
            eventListener = new TestProbe(actorSystem);
            environmentFactory = new WorkflowServiceImpl(
                factory,
                Collections.singletonList(eventListener.ref()),
                statusKeepingService,
                executionContext
            );
        }

        @Override
        public void close() {
            JavaTestKit.shutdownActorSystem(actorSystem);
        }
    }

    private static MutableModule<?> newModule() {
        return new MutableProxyModule().setDeclaration(DECLARATION);
    }

    private static List<MutableOverride> newOverrides() {
        return Collections.singletonList(
            new MutableOverride()
                .setTargets(Collections.singletonList(
                    new MutableExecutionTraceTarget()
                        .setExecutionTrace(":out:sum")
                ))
                .setDeclaredAnnotations(Collections.singletonList(
                    new MutableAnnotation()
                        .setDeclaration(CloudKeeperSerialization.class.getName())
                        .setEntries(Collections.singletonList(
                            new MutableAnnotationEntry()
                                .setKey("value")
                                .setValue("foo.Marshaler")
                        ))
                ))
        );
    }

    /**
     * Verifies that {@link WorkflowService#create(String, boolean)} creates a proper
     * {@link CloudKeeperEnvironment} instance, which creates proper {@link WorkflowExecutionBuilder} instances, which
     * creates proper {@link WorkflowExecution} instances.
     */
    @Test
    public void create() {
        try (TestCase testCase = new TestCase("create")) {
            CloudKeeperEnvironment environment = testCase.environmentFactory.create(PREFIX, true);
            MutableProxyModule module = new MutableProxyModule().setDeclaration(DECLARATION);
            WorkflowExecutionBuilder workflowExecutionBuilder = environment.newWorkflowExecutionBuilder(module);
            Map<SimpleName, Object> inputs = Collections.singletonMap(SimpleName.identifier(IN_PORT), IN_PORT_ARG);
            workflowExecutionBuilder.setInputs(inputs);
            WorkflowExecution workflowExecution = workflowExecutionBuilder.start();

            // Verify that the inputs have been propagated to the underlying WorkflowExecutionBuilder
            @Nullable MockWorkflowExecutionBuilder mockBuilder = testCase.factory.lastWorkflowBuilder;
            Assert.assertNotNull(mockBuilder);
            Assert.assertEquals(mockBuilder.inputValues, inputs);

            AtomicReference<Object> receivedOutput = new AtomicReference<>();

            // Verify that WorkflowExecution#start() also called the #start() method of the underlying instance.
            @Nullable MockWorkflowExecution mockWorkflowExecution = testCase.factory.mockWorkflowExecution;
            Assert.assertNotNull(mockWorkflowExecution);

            // Verify that a StartExecutionEvent is sent once the workflow execution has started
            TestProbe eventListener = testCase.eventListener;
            Assert.assertFalse(eventListener.msgAvailable());
            mockWorkflowExecution.setExecutionId(EXECUTION_ID);
            Assert.assertFalse(eventListener.msgAvailable());
            RuntimeAnnotatedExecutionTrace executionTrace = Mockito.mock(RuntimeAnnotatedExecutionTrace.class);
            mockWorkflowExecution.setExecutionTrace(executionTrace);
            eventListener.expectMsg(new StartExecutionEvent(EXECUTION_ID, executionTrace, PREFIX));
            Assert.assertFalse(eventListener.msgAvailable());

            // Verify that initially the output future is not completed, but also that it can be completed before the
            // execution finished
            workflowExecution.whenHasOutput(OUT_PORT, (throwable, value) -> receivedOutput.set(value));
            Assert.assertNull(receivedOutput.get());
            mockWorkflowExecution.setOutput(OUT_PORT, OUT_PORT_RESULT);
            Assert.assertSame(receivedOutput.get(), OUT_PORT_RESULT);

            // Verify that a StopExecutionEvent is sent when the workflow execution finishes
            mockWorkflowExecution.setSuccess();
            eventListener.expectMsg(new StopExecutionEvent(EXECUTION_ID));
            Assert.assertFalse(eventListener.msgAvailable());
        }
    }

    /**
     * Verifies that {@link WorkflowService#startExecution(ExecuteWorkflowRequest)} starts a proper
     * {@link WorkflowExecution}. Then verifies that {@link WorkflowService#getExecutionStatus(long)},
     * {@link WorkflowService#stopExecutionId(long)} and
     * {@link WorkflowService#getActiveExecutions()} work as expected.
     */
    @Test
    public void startExecution() throws Exception {
        try (TestCase testCase = new TestCase("startExecution")) {
            ExecuteWorkflowRequest executeWorkflowRequest = new ExecuteWorkflowRequest()
                .setBundleIdentifiers(BUNDLE_IDENTIFIERS)
                .setCleaningRequested(true)
                .setModule(newModule())
                .setOverrides(newOverrides())
                .setPrefix(PREFIX);
            WorkflowService environmentFactory = testCase.environmentFactory;
            WorkflowExecution workflowExecution = environmentFactory.startExecution(executeWorkflowRequest);

            // Verify that all parameters set in the request have been properly passed to the underlying
            // CloudKeeperEnvironment, WorkflowExecutionBuilder, and WorkflowExecution objects
            @Nullable MockCloudKeeperEnvironment mockCloudKeeperEnvironment
                = testCase.factory.lastCloudKeeperEnvironment;
            Assert.assertNotNull(mockCloudKeeperEnvironment);
            Assert.assertEquals(mockCloudKeeperEnvironment.prefix, PREFIX);
            Assert.assertEquals(mockCloudKeeperEnvironment.cleaningRequested, true);

            @Nullable MockWorkflowExecutionBuilder mockBuilder = testCase.factory.lastWorkflowBuilder;
            Assert.assertNotNull(mockBuilder);
            Assert.assertEquals(mockBuilder.bundleIdentifiers, BUNDLE_IDENTIFIERS);
            Assert.assertEquals(mockBuilder.module, newModule());
            Assert.assertEquals(mockBuilder.overrides, newOverrides());

            @Nullable MockWorkflowExecution mockWorkflowExecution = testCase.factory.mockWorkflowExecution;
            Assert.assertNotNull(mockWorkflowExecution);

            // Verify that the execution is not yet "active".
            try {
                environmentFactory.getExecutionStatus(EXECUTION_ID).get();
                Assert.fail();
            } catch (ExecutionException exception) {
                Assert.assertTrue(exception.getCause().getMessage().contains(String.valueOf(EXECUTION_ID)));
            }

            // Now simulate that the execution starts, i.e., complete the execution ID and trace futures. Then verify
            // that the execution is active
            mockWorkflowExecution.setExecutionId(EXECUTION_ID);
            RuntimeAnnotatedExecutionTrace executionTrace = Mockito.mock(RuntimeAnnotatedExecutionTrace.class);
            mockWorkflowExecution.setExecutionTrace(executionTrace);
            ExecutionStatus expectedExecutionStatus = new ExecutionStatus()
                .setStatus(ExecutionStatus.Status.RUNNING)
                .setExecutionId(EXECUTION_ID)
                .setRequest(executeWorkflowRequest);
            Assert.assertEquals(environmentFactory.getExecutionStatus(EXECUTION_ID).get(), expectedExecutionStatus);
            Assert.assertEquals(
                environmentFactory.getActiveExecutions().getList(), Collections.singletonList(expectedExecutionStatus));

            // Stop the execution and verify that it is no longer active. Also verify that the workflow execution was
            // completed exceptionally
            AtomicReference<Throwable> failure = new AtomicReference<>();
            workflowExecution.whenExecutionFinished(
                (@Nullable Throwable throwable, @Nullable Void value) -> failure.set(throwable)
            );
            Assert.assertNull(failure.get());

            environmentFactory.stopExecutionId(EXECUTION_ID);
            // Verify that StatusKeepingService was informed immediately
            @Nullable ExecutionStatus status = testCase.statusKeepingService.executionStatusMap.get(EXECUTION_ID);
            Assert.assertNotNull(status);
            @Nullable String failureDescription = status.getFailureDescription();
            Assert.assertNotNull(failureDescription);
            Assert.assertTrue(failureDescription.contains("cancel"));

            // Make sure event listener acknowledges the expected StopExecutionEvent message by sending an arbitrary
            // reply. (First, we have to skip the StartExecutionEvent message already in the queue.)
            TestProbe eventListener = testCase.eventListener;
            eventListener.expectMsgClass(StartExecutionEvent.class);
            eventListener.expectMsg(new StopExecutionEvent(EXECUTION_ID));
            eventListener.lastSender().tell(Boolean.TRUE, eventListener.ref());
            testCase.executor.executeAll();

            @Nullable Throwable throwable = failure.get();
            Assert.assertNotNull(throwable);
            Assert.assertTrue(throwable.getMessage().contains("cancel"));
            Assert.assertTrue(environmentFactory.getActiveExecutions().getList().isEmpty());

            // Verify that getExecutionStatus now returns an updated status
            expectedExecutionStatus.setStatus(ExecutionStatus.Status.FAILED);
            expectedExecutionStatus.setFailureDescription(Throwables.executionTraceToString(throwable));
            Assert.assertEquals(environmentFactory.getExecutionStatus(EXECUTION_ID).get(), expectedExecutionStatus);
        }
    }

    @Test
    public void startExecutionIllegalArguments() {
        try (TestCase testCase = new TestCase("startExecutionIllegalArguments")) {
            WorkflowService environmentFactory = testCase.environmentFactory;

            try {
                environmentFactory.startExecution(null);
                Assert.fail();
            } catch (NullPointerException ignored) { }

            ExecuteWorkflowRequest request = new ExecuteWorkflowRequest();
            try {
                environmentFactory.startExecution(request);
                Assert.fail();
            } catch (IllegalArgumentException ignored) { }

            request.setPrefix(PREFIX);
            try {
                environmentFactory.startExecution(request);
                Assert.fail();
            } catch (IllegalArgumentException ignored) { }
        }
    }

    private static final class MockCloudKeeperEnvironmentFactory implements CloudKeeperEnvironmentFactory {
        @Nullable private MockCloudKeeperEnvironment lastCloudKeeperEnvironment;
        @Nullable private MockWorkflowExecutionBuilder lastWorkflowBuilder;
        @Nullable private MockWorkflowExecution mockWorkflowExecution;

        @Override
        public CloudKeeperEnvironment create(String prefix, boolean cleaningRequested) {
            lastCloudKeeperEnvironment = new MockCloudKeeperEnvironment(this, prefix, cleaningRequested);
            return lastCloudKeeperEnvironment;
        }
    }

    private static final class MockCloudKeeperEnvironment implements CloudKeeperEnvironment {
        private final MockCloudKeeperEnvironmentFactory environmentFactory;
        private final String prefix;
        private final boolean cleaningRequested;

        private MockCloudKeeperEnvironment(MockCloudKeeperEnvironmentFactory environmentFactory, String prefix,
                boolean cleaningRequested) {
            this.environmentFactory = environmentFactory;
            this.prefix = prefix;
            this.cleaningRequested = cleaningRequested;
        }

        @Override
        public WorkflowExecutionBuilder newWorkflowExecutionBuilder(BareModule module) {
            environmentFactory.lastWorkflowBuilder = new MockWorkflowExecutionBuilder(environmentFactory, module);
            return environmentFactory.lastWorkflowBuilder;
        }
    }

    private static final class MockWorkflowExecutionBuilder implements WorkflowExecutionBuilder {
        private final MockCloudKeeperEnvironmentFactory environmentFactory;
        private final BareModule module;
        @Nullable private List<URI> bundleIdentifiers;
        @Nullable private List<? extends BareOverride> overrides;
        @Nullable private Map<SimpleName, Object> inputValues;

        private MockWorkflowExecutionBuilder(MockCloudKeeperEnvironmentFactory environmentFactory, BareModule module) {
            this.environmentFactory = environmentFactory;
            this.module = module;
        }

        @Override
        public WorkflowExecutionBuilder setBundleIdentifiers(List<URI> bundleIdentifiers) {
            this.bundleIdentifiers = bundleIdentifiers;
            return this;
        }

        @Override
        public WorkflowExecutionBuilder setOverrides(List<? extends BareOverride> overrides) {
            this.overrides = overrides;
            return this;
        }

        @Override
        public WorkflowExecutionBuilder setInputs(Map<SimpleName, Object> inputValues) {
            this.inputValues = inputValues;
            return this;
        }

        @Override
        public WorkflowExecution start() {
            environmentFactory.mockWorkflowExecution = new MockWorkflowExecution();
            return environmentFactory.mockWorkflowExecution;
        }
    }

    private static final class MockWorkflowExecution implements WorkflowExecution {
        @Nullable private RuntimeAnnotatedExecutionTrace executionTrace;
        private long executionId = 0;
        private boolean running = true;
        @Nullable private Exception failure;
        private final List<OnActionComplete<RuntimeAnnotatedExecutionTrace>> traceCallbacks = new ArrayList<>();
        private final List<OnActionComplete<Long>> executionIdCallbacks = new ArrayList<>();
        private final List<OnActionComplete<Void>> finishCallbacks = new ArrayList<>();
        private final Map<String, Object> outputMap = new HashMap<>();
        private final Map<String, List<OnActionComplete<Object>>> outputCallbacks = new HashMap<>();

        @Override
        public long getStartTimeMillis() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean cancel() {
            if (running) {
                setFailure(new CancellationException());
                return true;
            } else {
                return false;
            }
        }

        void setExecutionTrace(RuntimeAnnotatedExecutionTrace executionTrace) {
            assert this.executionTrace == null;
            this.executionTrace = executionTrace;
            traceCallbacks.forEach(callback -> callback.complete(null, executionTrace));
            traceCallbacks.clear();
        }

        @Override
        public void whenHasRootExecutionTrace(
                OnActionComplete<RuntimeAnnotatedExecutionTrace> onHasRootExecutionTrace) {
            if (executionTrace != null) {
                onHasRootExecutionTrace.complete(null, executionTrace);
            } else {
                traceCallbacks.add(onHasRootExecutionTrace);
            }
        }

        private void setExecutionId(long executionId) {
            assert this.executionId <= 0 && executionId > 0;
            this.executionId = executionId;
            executionIdCallbacks.forEach(callback -> callback.complete(null, executionId));
            executionIdCallbacks.clear();
        }

        @Override
        public void whenHasExecutionId(OnActionComplete<Long> onHasExecutionId) {
            if (executionId > 0) {
                onHasExecutionId.complete(null, executionId);
            } else {
                executionIdCallbacks.add(onHasExecutionId);
            }
        }

        private void setSuccess() {
            assert running;
            running = false;
            finishCallbacks.forEach(callback -> callback.complete(null, null));
            finishCallbacks.clear();
        }

        private void setFailure(Exception exception) {
            assert running;
            running = false;
            failure = exception;
            Stream.concat(
                    Stream.of(traceCallbacks, executionIdCallbacks, finishCallbacks),
                    outputCallbacks.values().stream()
                )
                .flatMap(List::stream)
                .forEach(callback -> callback.complete(exception, null));
            Stream.of(traceCallbacks, executionIdCallbacks, finishCallbacks).forEach(List::clear);
            outputCallbacks.clear();
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        private List<OnActionComplete<Object>> getOutportCallbacks(String outPortName) {
            @Nullable List<OnActionComplete<Object>> callbacks = outputCallbacks.get(outPortName);
            if (callbacks == null) {
                callbacks = new ArrayList<>();
                outputCallbacks.put(outPortName, callbacks);
            }
            return callbacks;
        }

        private void setOutput(String outPortName, Object value) {
            assert !outputMap.containsKey(outPortName);
            outputMap.put(outPortName, value);
            List<OnActionComplete<Object>> outportCallbacks = getOutportCallbacks(outPortName);
            outportCallbacks.forEach(callback -> callback.complete(null, value));
            outportCallbacks.clear();
        }

        @Override
        public void whenHasOutput(String outPortName, OnActionComplete<Object> onHasOutput) {
            @Nullable Object outputValue = outputMap.get(outPortName);
            if (outputValue != null) {
                onHasOutput.complete(null, outputValue);
            } else {
                getOutportCallbacks(outPortName).add(onHasOutput);
            }
        }

        @Override
        public void whenHasFinishTimeMillis(OnActionComplete<Long> onHasFinishTimeMillis) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void whenExecutionFinished(OnActionComplete<Void> onExecutionFinished) {
            if (!running) {
                onExecutionFinished.complete(null, null);
            } else {
                finishCallbacks.add(onExecutionFinished);
            }
        }
    }

    private static final class MockStatusKeepingService implements StatusKeepingService {
        private final Map<Long, ExecutionStatus> executionStatusMap = new HashMap<>();

        @Override
        public CompletableFuture<ExecutionStatus> persistExecutionStatus(ExecutionStatus executionStatus) {
            executionStatusMap.put(executionStatus.getExecutionId(), executionStatus);
            return CompletableFuture.completedFuture(executionStatus);
        }

        @Override
        public CompletableFuture<ExecutionStatus> loadExecutionStatus(long executionId) {
            @Nullable ExecutionStatus executionStatus = executionStatusMap.get(executionId);
            if (executionStatus != null) {
                return CompletableFuture.completedFuture(executionStatus);
            } else {
                CompletableFuture<ExecutionStatus> future = new CompletableFuture<>();
                future.completeExceptionally(new UnknownExecutionIdException(executionId));
                return future;
            }
        }
    }
}
