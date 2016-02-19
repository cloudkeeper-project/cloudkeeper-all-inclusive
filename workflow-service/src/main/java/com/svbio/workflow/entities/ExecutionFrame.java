package com.svbio.workflow.entities;

import javax.annotation.Nullable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.ManyToOne;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

/**
 * CloudKeeper execution frame in a workflow execution.
 *
 * <p>An execution frame represents one stack frame in the CloudKeeper call stack.
 */
@Entity
@IdClass(ExecutionFrame.ID.class)
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
public class ExecutionFrame {
    public static final class ID implements Serializable {
        private static final long serialVersionUID = -6101351381044635658L;

        private long execution;
        @Nullable private String frame;

        @Override
        public boolean equals(@Nullable Object otherObject) {
            if (this == otherObject) {
                return true;
            } else if (otherObject == null || getClass() != otherObject.getClass()) {
                return false;
            }

            ID other = (ID) otherObject;
            return execution == other.execution && Objects.equals(frame, other.frame);
        }

        @Override
        public int hashCode() {
            return Objects.hash(execution, frame);
        }

        public long getExecution() {
            return execution;
        }

        public ID setExecution(long execution) {
            this.execution = execution;
            return this;
        }

        @Nullable
        public String getFrame() {
            return frame;
        }

        public ID setFrame(@Nullable String frame) {
            this.frame = frame;
            return this;
        }
    }

    public enum ModuleKind {
        INPUT,
        COMPOSITE,
        LOOP,
        SIMPLE
    }

    @Nullable private Execution execution;
    @Nullable private String frame;
    @Nullable private ModuleKind moduleKind;
    private long startTime;
    private long finishTime;
    @Nullable private Boolean successful = null;

    @Override
    public boolean equals(@Nullable Object otherObject) {
        if (this == otherObject) {
            return true;
        } else if (otherObject == null || getClass() != otherObject.getClass()) {
            return false;
        }

        ExecutionFrame other = (ExecutionFrame) otherObject;
        return Objects.equals(execution, other.execution)
            && Objects.equals(frame, other.frame)
            && moduleKind == other.moduleKind
            && startTime == other.startTime
            && finishTime == other.finishTime
            && Objects.equals(successful, other.successful);
    }

    @Override
    public int hashCode() {
        return Objects.hash(execution, frame, moduleKind, startTime, finishTime, successful);
    }

    /**
     * Returns the CloudKeeper workflow execution of this execution frame.
     */
    @Id
    @ManyToOne(optional = false)
    @Nullable
    public Execution getExecution() {
        return execution;
    }

    public ExecutionFrame setExecution(@Nullable Execution execution) {
        this.execution = execution;
        return this;
    }

    /**
     * Returns the identifier of this execution frame (also called execution trace).
     *
     * <p>This method returns an execution trace that is a valid string representation of a
     * {@link xyz.cloudkeeper.model.immutable.execution.ExecutionTrace} instance.
     *
     * @see xyz.cloudkeeper.model.immutable.execution.ExecutionTrace
     */
    @Id
    @Column(length = 1024)
    @Nullable
    public String getFrame() {
        return frame;
    }

    public ExecutionFrame setFrame(@Nullable String frame) {
        this.frame = frame;
        return this;
    }

    /**
     * Returns the kind of module corresponding to this execution frame.
     */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Nullable
    public ModuleKind getModuleKind() {
        return moduleKind;
    }

    public ExecutionFrame setModuleKind(@Nullable ModuleKind moduleKind) {
        this.moduleKind = moduleKind;
        return this;
    }

    @Temporal(TemporalType.TIMESTAMP)
    @Nullable
    public Date getStartTime() {
        return startTime == 0
            ? null
            : new Date(startTime);
    }

    public ExecutionFrame setStartTime(@Nullable Date startTime) {
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

    public ExecutionFrame setFinishTime(@Nullable Date finishTime) {
        this.finishTime = finishTime == null
            ? 0
            : finishTime.getTime();
        return this;
    }

    @Nullable
    public Boolean getSuccessful() {
        return successful;
    }

    public ExecutionFrame setSuccessful(@Nullable Boolean successful) {
        this.successful = successful;
        return this;
    }
}
