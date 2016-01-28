package com.svbio.workflow.api;

import javax.annotation.Nullable;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.io.Serializable;
import java.util.Objects;

/**
 * Status of an active workflow execution.
 */
@XmlRootElement(name = "execution-status")
@XmlType(propOrder = { "executionId", "request", "status", "failureDescription" })
public final class ExecutionStatus implements Serializable {
    private static final long serialVersionUID = 911345605836738649L;

    private long executionId;
    @Nullable private ExecuteWorkflowRequest request;
    private Status status = Status.RUNNING;
    @Nullable private String failureDescription;

    /**
     * Lifecycle status of a workflow execution.
     */
    public enum Status {
        /**
         * CloudKeeper has received the request to execute a workflow and has assigned an execution id.
         */
        RUNNING,

        /**
         * The workflow execution has finished successfully.
         */
        SUCCESSFUL,

        /**
         * The workflow execution has finished with an error.
         */
        FAILED
    }

    /**
     * Constructor for instance with default properties.
     */
    public ExecutionStatus() { }

    /**
     * Copy constructor.
     *
     * <p>The newly constructed instance is guaranteed to share no mutable state with the original instance (not even
     * transitively through multiple object references).
     *
     * @param original original instance that is to be copied
     */
    public ExecutionStatus(ExecutionStatus original) {
        executionId = original.executionId;
        request = original.request == null
            ? null
            : new ExecuteWorkflowRequest(original.request);
        status = original.status;
        failureDescription = original.failureDescription;
    }

    @Override
    public boolean equals(@Nullable Object otherObject) {
        if (this == otherObject) {
            return true;
        } else if (otherObject == null || getClass() != otherObject.getClass()) {
            return false;
        }

        ExecutionStatus other = (ExecutionStatus) otherObject;
        return executionId == other.executionId
            && Objects.equals(request, other.request)
            && status == other.status
            && Objects.equals(failureDescription, other.failureDescription);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executionId, request, status, failureDescription);
    }

    @XmlElement(name = "execution-id")
    public long getExecutionId() {
        return executionId;
    }

    public ExecutionStatus setExecutionId(long executionId) {
        this.executionId = executionId;
        return this;
    }

    @XmlElement
    @Nullable
    public ExecuteWorkflowRequest getRequest() {
        return request;
    }

    public ExecutionStatus setRequest(@Nullable ExecuteWorkflowRequest request) {
        this.request = request;
        return this;
    }

    @XmlElement
    public Status getStatus() {
        return status;
    }

    public ExecutionStatus setStatus(Status status) {
        Objects.requireNonNull(status);
        this.status = status;
        return this;
    }

    @XmlElement(name = "failure-description")
    @Nullable
    public String getFailureDescription() {
        return failureDescription;
    }

    public ExecutionStatus setFailureDescription(@Nullable String failureDescription) {
        this.failureDescription = failureDescription;
        return this;
    }
}
