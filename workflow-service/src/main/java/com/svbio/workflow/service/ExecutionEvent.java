package com.svbio.workflow.service;

import javax.annotation.Nullable;

/**
 * Abstract base class of events published by {@link com.svbio.cloudkeeper.model.api.WorkflowExecution} instances
 * descending from {@link com.svbio.workflow.api.WorkflowService}.
 */
abstract class ExecutionEvent {
    private final long executionId;

    ExecutionEvent(long executionId) {
        this.executionId = executionId;
    }

    @Override
    public boolean equals(@Nullable Object otherObject) {
        return this == otherObject || (
            otherObject != null
                && getClass() == otherObject.getClass()
                && executionId == ((ExecutionEvent) otherObject).executionId
        );
    }

    @Override
    public int hashCode() {
        return Long.hashCode(executionId);
    }

    public final long getExecutionId() {
        return executionId;
    }
}
