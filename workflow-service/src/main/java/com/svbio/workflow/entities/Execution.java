package com.svbio.workflow.entities;

import javax.annotation.Nullable;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.util.Date;
import java.util.Objects;

/**
 * CloudKeeper Workflow Execution.
 *
 * <p>Instances of this class represent a workflow execution, which was started when
 * {@link xyz.cloudkeeper.model.api.WorkflowExecutionBuilder#start()} was called.
 *
 * <p>Note: JPA 2.1 mandates that an "entity class must not be final" (§2.1).
 */
@Entity
public class Execution {
    private long id;
    @Nullable private String keyPrefix;
    @Nullable private String userName;
    private long startTime;
    private long finishTime;

    @Override
    public boolean equals(@Nullable Object otherObject) {
        if (this == otherObject) {
            return true;
        } else if (otherObject == null || getClass() != otherObject.getClass()) {
            return false;
        }

        Execution other = (Execution) otherObject;
        return id == other.id
            && Objects.equals(keyPrefix, other.keyPrefix)
            && Objects.equals(userName, other.userName)
            && startTime == other.startTime
            && finishTime == other.finishTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, keyPrefix, userName, startTime, finishTime);
    }

    @Override
    public String toString() {
        return String.format(
            "Execution %d (prefix: '%s', start: %d, finish: %d)", id, keyPrefix, startTime, finishTime);
    }

    /**
     * Returns the CloudKeeper execution id of this workflow execution.
     */
    @Id
    public long getId() {
        return id;
    }

    public Execution setId(long id) {
        this.id = id;
        return this;
    }

    /**
     * Returns the prefix of this workflow execution, which was part of the workflow-execution request.
     *
     * @see com.svbio.workflow.api.WorkflowService#create(String, boolean)
     */
    @Nullable
    public String getKeyPrefix() {
        return keyPrefix;
    }

    public Execution setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
        return this;
    }

    @Nullable
    public String getUserName() {
        return userName;
    }

    public Execution setUserName(@Nullable String userName) {
        this.userName = userName;
        return this;
    }

    /**
     * Returns the timestamp of when the workflow execution started.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Nullable
    public Date getStartTime() {
        return startTime == 0
            ? null
            : new Date(startTime);
    }

    public Execution setStartTime(@Nullable Date startTime) {
        this.startTime = startTime == null
            ? 0
            : startTime.getTime();
        return this;
    }

    @Temporal(TemporalType.TIMESTAMP)
    @Nullable
    public Date getFinishTime() {
        return finishTime == 0
            ? null
            : new Date(finishTime);
    }

    public Execution setFinishTime(@Nullable Date finishTime) {
        this.finishTime = finishTime == null
            ? 0
            : finishTime.getTime();
        return this;
    }
}
