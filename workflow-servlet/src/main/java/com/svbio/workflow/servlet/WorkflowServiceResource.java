package com.svbio.workflow.servlet;

import com.svbio.cloudkeeper.model.api.WorkflowExecution;
import com.svbio.workflow.api.ExecuteWorkflowRequest;
import com.svbio.workflow.api.ExecutionStatus;
import com.svbio.workflow.api.ExecutionStatusList;
import com.svbio.workflow.api.UnknownExecutionIdException;
import com.svbio.workflow.api.WorkflowService;

import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.ext.Providers;
import java.util.Objects;

/**
 * JAX-RS resource that provides a RESTful interface for {@link WorkflowService}.
 */
@Path("executor")
@Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
public final class WorkflowServiceResource {
    static final String EXECUTION_ID = "eid";
    private static final String EXECUTIONS_TEMPLATE = "executions";
    private static final String EXECUTION_ID_TEMPLATE = EXECUTIONS_TEMPLATE + "/{" + EXECUTION_ID + ": [0-9]+}";

    private final WorkflowService workflowService;

    /**
     * Constructor.
     *
     * <p>Root resource classes are instantiated by the JAX-RS runtime and MUST have a public constructor for which the
     * JAX-RS runtime can provide all parameter values (§3.1.2 of JAX-RS 2.0)
     *
     * @param providers will be used to lookup a context resolver for {@link WorkflowService}
     */
    public WorkflowServiceResource(@Context Providers providers) {
        workflowService = providers
            .getContextResolver(WorkflowService.class, MediaType.WILDCARD_TYPE)
            .getContext(null);
        Objects.requireNonNull(workflowService);
    }

    @Path(EXECUTION_ID_TEMPLATE)
    @GET
    public void getExecutionStatus(@Suspended final AsyncResponse asyncResponse,
            @PathParam(EXECUTION_ID) long executionID) {
        workflowService.getExecutionStatus(executionID).whenComplete(
            (@Nullable ExecutionStatus executionStatus, @Nullable Throwable throwable) -> {
                if (throwable != null) {
                    asyncResponse.resume(throwable);
                } else {
                    asyncResponse.resume(executionStatus);
                }
            }
        );
    }

    @Path(EXECUTION_ID_TEMPLATE)
    @DELETE
    public void stopExecutionID(@PathParam(EXECUTION_ID) long executionID) throws UnknownExecutionIdException {
        workflowService.stopExecutionId(executionID);
    }

    @Path(EXECUTIONS_TEMPLATE)
    @POST
    public void executeWorkflow(@Suspended final AsyncResponse asyncResponse,
            ExecuteWorkflowRequest executeWorkflowRequest) {
        WorkflowExecution workflowExecution = workflowService.startExecution(executeWorkflowRequest);
        workflowExecution.whenHasExecutionId((@Nullable Throwable throwable, @Nullable Long executionId) -> {
            if (throwable != null) {
                asyncResponse.resume(throwable);
            } else {
                asyncResponse.resume(
                    Response.seeOther(
                        UriBuilder.fromResource(getClass())
                            .path(getClass(), "getExecutionStatus")
                            .resolveTemplate(EXECUTION_ID, executionId)
                            .build()
                    )
                    .build()
                );
            }
        });
    }

    @GET
    @Path(EXECUTIONS_TEMPLATE)
    public ExecutionStatusList getListOfActiveWorkflows() {
        return workflowService.getActiveExecutions();
    }
}
