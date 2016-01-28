package com.svbio.workflow.service;

import com.svbio.workflow.api.ExecutionStatus;

import java.util.concurrent.CompletableFuture;

/**
 * Subservice that persists execution results.
 */
interface StatusKeepingService {
    /**
     * Persists the given execution status so that it may be retrieved later with {@link #loadExecutionStatus}.
     *
     * @param executionStatus the execution status to be persisted
     * @return future that will be completed with the argument in case of success and an {@link Exception} in case of
     *     failure
     * @throws IllegalArgumentException if the given execution status does not have an execution id
     */
    CompletableFuture<ExecutionStatus> persistExecutionStatus(ExecutionStatus executionStatus);

    /**
     * Returns a future that will be completed with the execution status for the given id.
     *
     * @param executionId the CloudKeeper execution id
     * @return future that will be completed with the execution status in case of success, or an
     *     {@link com.svbio.workflow.api.UnknownExecutionIdException} in case the execution id is unknown, or an
     *     {@link Exception} in case of any other failure
     */
    CompletableFuture<ExecutionStatus> loadExecutionStatus(long executionId);
}
