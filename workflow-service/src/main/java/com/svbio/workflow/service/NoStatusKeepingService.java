package com.svbio.workflow.service;

import com.svbio.workflow.api.ExecutionStatus;
import com.svbio.workflow.api.UnknownExecutionIdException;

import java.util.concurrent.CompletableFuture;

/**
 * Implementation of {@link StatusKeepingService} that immediately forgets about any execution status.
 */
final class NoStatusKeepingService implements StatusKeepingService {
    @Override
    public CompletableFuture<ExecutionStatus> persistExecutionStatus(ExecutionStatus executionStatus) {
        return CompletableFuture.completedFuture(executionStatus);
    }

    @Override
    public CompletableFuture<ExecutionStatus> loadExecutionStatus(long executionId) {
        CompletableFuture<ExecutionStatus> future = new CompletableFuture<>();
        future.completeExceptionally(new UnknownExecutionIdException(executionId));
        return future;
    }
}
