package com.svbio.workflow.servlet;

import cloudkeeper.annotations.CloudKeeperSerialization;
import com.svbio.cloudkeeper.model.api.CloudKeeperEnvironment;
import com.svbio.cloudkeeper.model.api.WorkflowExecution;
import com.svbio.cloudkeeper.model.beans.element.annotation.MutableAnnotation;
import com.svbio.cloudkeeper.model.beans.element.annotation.MutableAnnotationEntry;
import com.svbio.cloudkeeper.model.beans.element.module.MutableModule;
import com.svbio.cloudkeeper.model.beans.element.module.MutableProxyModule;
import com.svbio.cloudkeeper.model.beans.execution.MutableExecutionTraceTarget;
import com.svbio.cloudkeeper.model.beans.execution.MutableOverride;
import com.svbio.cloudkeeper.model.runtime.execution.RuntimeAnnotatedExecutionTrace;
import com.svbio.workflow.api.ExecuteWorkflowRequest;
import com.svbio.workflow.api.ExecutionStatus;
import com.svbio.workflow.api.ExecutionStatusList;
import com.svbio.workflow.api.UnknownExecutionIdException;
import com.svbio.workflow.api.WorkflowService;
import com.svbio.workflow.service.WorkflowServiceComponent;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.moxy.json.MoxyJsonConfig;
import org.glassfish.jersey.moxy.json.MoxyJsonFeature;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServlet;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ITWorkflowServletModule {
    private static final List<URI> BUNDLE_IDENTIFIERS = Collections.singletonList(URI.create("x-test:foo"));
    private static final String DECLARATION = "foo.Module";
    private static final String PREFIX = "prefix";
    private static final String LOCALHOST = "localhost";
    private static final int HTTP_PORT = 8081;

    private final MockWorkflowService mockWorkflowService = new MockWorkflowService();
    @Nullable private Server server;
    @Nullable private Client jaxrsClient;

    @BeforeClass
    public void setup() throws Exception {
        MockWorkflowServiceComponent mockWorkflowServiceComponent
            = new MockWorkflowServiceComponent(mockWorkflowService);
        WorkflowServletComponent workflowServletComponent = DaggerWorkflowServletComponent.builder()
            .workflowServiceComponent(mockWorkflowServiceComponent)
            .build();
        HttpServlet httpServlet = workflowServletComponent.getServlet();

        server = new Server(HTTP_PORT);
        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(httpServlet), "/api/*");
        server.setHandler(handler);
        server.start();

        jaxrsClient = ClientBuilder.newClient(
            new ClientConfig()
                .property(CommonProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true)
                .register(MoxyJsonFeature.class)
                .register(new MoxyJsonConfig().setIncludeRoot(false))
        );
    }

    @AfterClass
    public void tearDown() throws Exception {
        assert server != null;
        server.stop();
        server.join();
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

    private static ExecuteWorkflowRequest newExecuteWorkflowRequest() {
        return new ExecuteWorkflowRequest()
            .setBundleIdentifiers(BUNDLE_IDENTIFIERS)
            .setModule(newModule())
            .setOverrides(newOverrides())
            .setPrefix(PREFIX)
            .setCleaningRequested(true);
    }

    static UriBuilder uriBuilder(String name, Class<?>... parameterTypes) {
        try {
            return UriBuilder.fromPath("api")
                .path(WorkflowServiceResource.class)
                .path(WorkflowServiceResource.class.getMethod(name, parameterTypes))
                .port(HTTP_PORT)
                .host(LOCALHOST)
                .scheme("http");
        } catch (NoSuchMethodException exception) {
            throw new AssertionError("Could not build URI.", exception);
        }
    }

    /**
     * Verifies that the REST resources
     * {@link WorkflowServiceResource#executeWorkflow(AsyncResponse, ExecuteWorkflowRequest)} and
     * {@link WorkflowServiceResource#getExecutionStatus(AsyncResponse, long)} are correctly mapped to the underlying
     * {@link WorkflowService}.
     */
    @Test
    public void executeWorkflow() throws NoSuchMethodException {
        assert jaxrsClient != null;

        // First try to start a workflow knowing that the MockWorkflowService will report an error. This should fail.
        ExecuteWorkflowRequest request = newExecuteWorkflowRequest();
        URI target = uriBuilder("executeWorkflow", AsyncResponse.class, ExecuteWorkflowRequest.class).build();
        Response failedExecuteResponse = jaxrsClient
            .target(target)
            .request()
            .buildPost(Entity.entity(request, MediaType.APPLICATION_XML_TYPE))
            .invoke();
        Assert.assertEquals(failedExecuteResponse.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());

        // Now try again knowing that the MockWorkflowService will accept the request
        long executionId = 3;
        mockWorkflowService.nextExecutionId = executionId;
        Response executeResponse = jaxrsClient
            .target(uriBuilder("executeWorkflow", AsyncResponse.class, ExecuteWorkflowRequest.class).build())
            .request()
            .buildPost(Entity.entity(request, MediaType.APPLICATION_XML_TYPE))
            .property(ClientProperties.FOLLOW_REDIRECTS, false)
            .invoke();
        Assert.assertEquals(mockWorkflowService.lastStartExecution, request);

        // The HTTP server should have replied with a redirect
        @Nullable String location = executeResponse.getHeaderString("Location");
        Assert.assertNotNull(location, "Expected HTTP redirection after submitting workflow execution request.");
        URI redirect = uriBuilder("getExecutionStatus", AsyncResponse.class, long.class)
            .resolveTemplate(WorkflowServiceResource.EXECUTION_ID, executionId)
            .build();
        Assert.assertTrue(location.endsWith(redirect.toString()));

        // Also, the ExecutionStatus should now be available under the redirected location
        ExecutionStatus expectedExecutionStatus = new ExecutionStatus()
            .setStatus(ExecutionStatus.Status.RUNNING)
            .setExecutionId(executionId)
            .setRequest(request);
        mockWorkflowService.executionStatus = expectedExecutionStatus;
        ExecutionStatus executionStatus = jaxrsClient.target(location)
            .request()
            .buildGet()
            .invoke(ExecutionStatus.class);
        Assert.assertEquals(executionStatus, expectedExecutionStatus);

        // Finally verify case where MockWorkflowService is not aware of execution ID
        mockWorkflowService.executionStatus = null;
        Response failedStatusResponse = jaxrsClient.target(location)
            .request()
            .get();
        Assert.assertEquals(failedStatusResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    /**
     * Verifies {@link WorkflowServiceResource#stopExecutionID(long)}.
     */
    @Test
    public void stopExecutionID() throws NoSuchMethodException {
        assert jaxrsClient != null;

        long executionId = 3;
        mockWorkflowService.executionStatus = new ExecutionStatus().setExecutionId(executionId);
        mockWorkflowService.lastStoppedExecutionId = 0;
        URI target = uriBuilder("stopExecutionID", long.class)
            .resolveTemplate(WorkflowServiceResource.EXECUTION_ID, executionId)
            .build();
        Response successResponse = jaxrsClient
            .target(target)
            .request()
            .delete();
        Assert.assertEquals(successResponse.getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        Assert.assertEquals(mockWorkflowService.lastStoppedExecutionId, executionId);

        // Verify exception in case the execution id is unknown
        mockWorkflowService.executionStatus = null;
        Response failedResponse = jaxrsClient
            .target(target)
            .request()
            .delete();
        Assert.assertEquals(failedResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    /**
     * Verifies {@link WorkflowServiceResource#getListOfActiveWorkflows()}.
     */
    @Test
    public void getListOfActiveWorkflows() throws NoSuchMethodException {
        assert jaxrsClient != null;

        mockWorkflowService.executionStatusList = new ExecutionStatusList()
            .setList(Collections.singletonList(
                new ExecutionStatus()
                    .setExecutionId(4)
                    .setStatus(ExecutionStatus.Status.FAILED)
                    .setFailureDescription("foo")
            ));
        ExecutionStatusList executionStatusList = jaxrsClient.target(uriBuilder("getListOfActiveWorkflows").build())
            .request()
            .buildGet()
            .invoke(ExecutionStatusList.class);
        Assert.assertEquals(executionStatusList, mockWorkflowService.executionStatusList);
    }

    static UnsupportedOperationException newUnsupportedOperationException() {
        return new UnsupportedOperationException("Not needed for this test.");
    }

    private static final class MockWorkflowServiceComponent implements WorkflowServiceComponent {
        private final MockWorkflowService workflowService;

        private MockWorkflowServiceComponent(MockWorkflowService workflowService) {
            this.workflowService = workflowService;
        }

        @Override
        public WorkflowService getWorkflowService() {
            return workflowService;
        }
    }

    private static final class MockWorkflowService implements WorkflowService {
        private long nextExecutionId;
        @Nullable private ExecuteWorkflowRequest lastStartExecution;
        private long lastStoppedExecutionId;
        @Nullable private ExecutionStatus executionStatus;
        @Nullable private ExecutionStatusList executionStatusList;

        @Override
        public CloudKeeperEnvironment create(String prefix, boolean cleaningRequested) {
            throw newUnsupportedOperationException();
        }

        @Override
        public WorkflowExecution startExecution(ExecuteWorkflowRequest request) {
            lastStartExecution = request;
            return new MockWorkflowExecution(nextExecutionId);
        }

        @Override
        public CompletableFuture<ExecutionStatus> getExecutionStatus(long executionId) {
            if (executionStatus != null && executionStatus.getExecutionId() == executionId) {
                return CompletableFuture.completedFuture(executionStatus);
            } else {
                CompletableFuture<ExecutionStatus> future = new CompletableFuture<>();
                future.completeExceptionally(new UnknownExecutionIdException(executionId));
                return future;
            }
        }

        @Override
        public void stopExecutionId(long executionId) throws UnknownExecutionIdException {
            if (executionStatus != null && executionStatus.getExecutionId() == executionId) {
                lastStoppedExecutionId = executionId;
            } else {
                throw new UnknownExecutionIdException(executionId);
            }
        }

        @Override
        public ExecutionStatusList getActiveExecutions() {
            assert executionStatusList != null;
            return executionStatusList;
        }
    }

    private static final class MockWorkflowExecution implements WorkflowExecution {
        private final long executionId;

        private MockWorkflowExecution(long executionId) {
            this.executionId = executionId;
        }

        @Override
        public long getStartTimeMillis() {
            throw newUnsupportedOperationException();
        }

        @Override
        public boolean cancel() {
            throw newUnsupportedOperationException();
        }

        @Override
        public void whenHasRootExecutionTrace(OnActionComplete<RuntimeAnnotatedExecutionTrace> onHasRootExecutionTrace) {
            throw newUnsupportedOperationException();
        }

        @Override
        public void whenHasExecutionId(OnActionComplete<Long> onHasExecutionId) {
            if (executionId > 0) {
                onHasExecutionId.complete(null, executionId);
            } else {
                onHasExecutionId.complete(new ExpectedException(), null);
            }
        }

        @Override
        public boolean isRunning() {
            throw newUnsupportedOperationException();
        }

        @Override
        public void whenHasOutput(String outPortName, OnActionComplete<Object> onHasOutput) {
            throw newUnsupportedOperationException();
        }

        @Override
        public void whenHasFinishTimeMillis(OnActionComplete<Long> onHasFinishTimeMillis) {
            throw newUnsupportedOperationException();
        }

        @Override
        public void whenExecutionFinished(OnActionComplete<Void> onExecutionFinished) {
            throw newUnsupportedOperationException();
        }
    }

    private static final class ExpectedException extends Exception {
        private static final long serialVersionUID = -7436050844707068610L;
    }
}
