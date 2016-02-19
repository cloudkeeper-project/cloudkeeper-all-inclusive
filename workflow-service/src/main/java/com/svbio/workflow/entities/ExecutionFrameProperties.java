package com.svbio.workflow.entities;

import javax.annotation.Nullable;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.OneToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import java.util.Date;
import java.util.Objects;

@Entity
@IdClass(ExecutionFrame.ID.class)
@DiscriminatorColumn(name = "workerkind", discriminatorType = DiscriminatorType.STRING)
public abstract class ExecutionFrameProperties<D extends ExecutionFrameProperties<D>> {
    @Nullable private ExecutionFrame executionFrame;
    private long dispatchTime = 0;
    @Nullable private String workerName;
    private long workerStartTime = 0;
    private long workerFinishTime = 0;

    @Override
    public boolean equals(@Nullable Object otherObject) {
        if (this == otherObject) {
            return true;
        } else if (otherObject == null || getClass() != otherObject.getClass()) {
            return false;
        }

        ExecutionFrameProperties<?> other = (ExecutionFrameProperties<?>) otherObject;
        return Objects.equals(executionFrame, other.executionFrame)
            && dispatchTime == other.dispatchTime
            && Objects.equals(workerName, other.workerName)
            && workerStartTime == other.workerStartTime
            && workerFinishTime == other.workerFinishTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(executionFrame, dispatchTime, workerName, workerStartTime, workerFinishTime);
    }

    @Id
    @OneToOne(optional = false)
    @Nullable
    public ExecutionFrame getExecutionFrame() {
        return executionFrame;
    }

    public D setExecutionFrame(@Nullable ExecutionFrame executionFrame) {
        this.executionFrame = executionFrame;
        return self();
    }

    @Transient
    @SuppressWarnings("unchecked")
    protected final D self() {
        return (D) this;
    }

    /**
     * Returns an undefined value. This property is no longer used in CloudKeeper 2.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Nullable
    public Date getDispatchTime() {
        return dispatchTime == 0
            ? null
            : new Date(dispatchTime);
    }

    public D setDispatchTime(@Nullable Date dispatchTime) {
        this.dispatchTime = dispatchTime == null
            ? 0
            : dispatchTime.getTime();
        return self();
    }

    /**
     * Returns the name of the implementing class of
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutor} used to execute the simple module.
     */
    @Nullable
    public String getWorkerName() {
        return workerName;
    }

    public D setWorkerName(@Nullable String workerName) {
        this.workerName = workerName;
        return self();
    }

    /**
     * Returns the timestamp immediately after
     * {@link xyz.cloudkeeper.simple.LocalSimpleModuleExecutor#submit(xyz.cloudkeeper.model.api.RuntimeStateProvider, scala.concurrent.Future)}
     * was called.
     *
     * <p>The returned timestamp is the value of property
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutor#SUBMISSION_TIME_MILLIS} in the
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutorResult} that the future returned by
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutor#submit(xyz.cloudkeeper.model.api.RuntimeStateProvider, scala.concurrent.Future)}
     * was completed with.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Nullable
    public Date getWorkerStartTime() {
        return workerStartTime == 0
            ? null
            : new Date(workerStartTime);
    }

    public D setWorkerStartTime(@Nullable Date workerStartTime) {
        this.workerStartTime = workerStartTime == null
            ? 0
            : workerStartTime.getTime();
        return self();
    }

    /**
     * Returns the timestamp immediately before the future returned by
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutor#submit(xyz.cloudkeeper.model.api.RuntimeStateProvider, scala.concurrent.Future)}
     * was completed.
     *
     * <p>The returned timestamp is the value of property
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutor#COMPLETION_TIME_MILLIS} in the
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutorResult} that the future returned by
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutor#submit(xyz.cloudkeeper.model.api.RuntimeStateProvider, scala.concurrent.Future)}
     * was completed with.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Nullable
    public Date getWorkerFinishTime() {
        return workerFinishTime == 0
            ? null
            : new Date(workerFinishTime);
    }

    public D setWorkerFinishTime(@Nullable Date workerFinishTime) {
        this.workerFinishTime = workerFinishTime == null
            ? 0
            : workerFinishTime.getTime();
        return self();
    }
}
