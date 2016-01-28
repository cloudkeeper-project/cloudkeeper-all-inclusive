package com.svbio.workflow.entities;

import javax.annotation.Nullable;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Lob;
import javax.persistence.OneToOne;
import java.util.Objects;

@Entity
@IdClass(ExecutionFrame.ID.class)
public class ExecutionFrameError {
    @Nullable private ExecutionFrame executionFrame;
    @Nullable private String errorMessage;

    @Override
    public boolean equals(@Nullable Object otherObject) {
        if (this == otherObject) {
            return true;
        } else if (otherObject == null || getClass() != otherObject.getClass()) {
            return false;
        }

        ExecutionFrameError other = (ExecutionFrameError) otherObject;
        return Objects.equals(executionFrame, other.executionFrame)
            && Objects.equals(errorMessage, other.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executionFrame, errorMessage);
    }

    @Id
    @OneToOne(optional = false)
    @Nullable
    public ExecutionFrame getExecutionFrame() {
        return executionFrame;
    }

    public ExecutionFrameError setExecutionFrame(@Nullable ExecutionFrame executionFrame) {
        this.executionFrame = executionFrame;
        return this;
    }

    @Lob
    @Nullable
    public String getErrorMessage() {
        return errorMessage;
    }

    public ExecutionFrameError setErrorMessage(@Nullable String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
    }
}
