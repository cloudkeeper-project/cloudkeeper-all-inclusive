package com.svbio.workflow.api;

import xyz.cloudkeeper.model.api.CloudKeeperEnvironment;
import xyz.cloudkeeper.model.api.WorkflowExecution;

import java.util.concurrent.CompletableFuture;

/**
 * Workflow-execution service.
 */
public interface WorkflowService {
    /**
     * Creates and returns a {@link CloudKeeperEnvironment}.
     *
     * @param prefix prefix for the staging area, the meaning of prefix depends on the configured staging-area service
     * @param cleaningRequested whether intermediate results should be removed from the staging area as soon as they are
     *     no longer needed
     * @return the new {@link CloudKeeperEnvironment}
     */
    CloudKeeperEnvironment create(String prefix, boolean cleaningRequested);

    /**
     * Requests the execution of a CloudKeeper workflow.
     *
     * <p>The effects of this method are equivalent to:
     * <ul><li>
     *     creating a {@link CloudKeeperEnvironment} with method {@link #create(String, boolean)} and arguments
     *     {@code request.getPrefix()} and {@code request.isCleaningRequested()},
     * </li><li>
     *     creating a {@link xyz.cloudkeeper.model.api.WorkflowExecutionBuilder} with method
     *     {@link CloudKeeperEnvironment#newWorkflowExecutionBuilder(xyz.cloudkeeper.model.bare.element.module.BareModule)}
     *     and argument {@code request.getModule()}, and
     * </li><li>
     *     setting bundle identifiers and overrides accordingly.
     * </li></ul>
     *
     * @param request the workflow execution request
     * @return the {@link WorkflowExecution} instance returned by
     *     {@link xyz.cloudkeeper.model.api.WorkflowExecutionBuilder#start}
     * @throws IllegalArgumentException if the module or the prefix properties of the given request are {@code null}
     */
    WorkflowExecution startExecution(ExecuteWorkflowRequest request);

    /**
     * Returns the execution status of the given execution id.
     *
     * <p>The returned {@link ExecutionStatus} instance is not backed by internal state. Workflow-execution updates are
     * not reflected in the returned instance, or vice versa.
     *
     * @param executionId execution id
     * @return a future that will be completed with the execution status on success, an
     *     {@link UnknownExecutionIdException} if the given execution id does not belong to an
     *     active workflow execution, and {@link Exception} in case of any other failure
     */
    CompletableFuture<ExecutionStatus> getExecutionStatus(long executionId);

    /**
     * Stops the execution with the given execution id.
     *
     * @param executionId execution id
     * @throws UnknownExecutionIdException if the given execution id does not belong to an active workflow execution
     */
    void stopExecutionId(long executionId) throws UnknownExecutionIdException;

    /**
     * Returns a list of currently active workflow executions.
     *
     * <p>The returned object is not backed by internal state. Workflow-execution updates are not reflected in the
     * returned object, or vice versa.
     *
     * @return the list of currently active workflow executions
     */
    ExecutionStatusList getActiveExecutions();
}
