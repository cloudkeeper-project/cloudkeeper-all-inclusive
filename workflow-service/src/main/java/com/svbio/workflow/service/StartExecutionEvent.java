package com.svbio.workflow.service;

import com.svbio.cloudkeeper.model.runtime.execution.RuntimeAnnotatedExecutionTrace;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Event to indicate that the execution of a workflow has started.
 *
 * <p>No response to this message is expected.
 */
final class StartExecutionEvent extends ExecutionEvent {
    private final RuntimeAnnotatedExecutionTrace rootTrace;
    private final String prefix;

    StartExecutionEvent(long executionId, RuntimeAnnotatedExecutionTrace rootTrace, String prefix) {
        super(executionId);
        Objects.requireNonNull(rootTrace);
        Objects.requireNonNull(prefix);
        this.rootTrace = rootTrace;
        this.prefix = prefix;
    }

    @Override
    public boolean equals(@Nullable Object otherObject) {
        if (this == otherObject) {
            return true;
        } else if (!super.equals(otherObject)) {
            return false;
        }

        StartExecutionEvent other = (StartExecutionEvent) otherObject;
        return rootTrace.equals(other.rootTrace)
            && prefix.equals(other.prefix);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(rootTrace, prefix);
    }

    public RuntimeAnnotatedExecutionTrace getRootTrace() {
        return rootTrace;
    }

    public String getPrefix() {
        return prefix;
    }
}
