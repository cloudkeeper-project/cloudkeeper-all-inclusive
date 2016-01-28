package com.svbio.workflow.api;

import javax.annotation.Nullable;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * List of active workflow executions.
 */
@XmlRootElement(name = "execution-status-list")
public final class ExecutionStatusList implements Serializable {
    private static final long serialVersionUID = -3214966772656946291L;

    private final ArrayList<ExecutionStatus> list = new ArrayList<>();

    /**
     * Constructor for instance with default properties.
     */
    public ExecutionStatusList() { }

    /**
     * Copy constructor.
     *
     * <p>The newly constructed instance is guaranteed to share no mutable state with the original instance (not even
     * transitively through multiple object references).
     *
     * @param original original instance that is to be copied
     */
    public ExecutionStatusList(ExecutionStatusList original) {
        original.getList().forEach(executionStatus -> list.add(new ExecutionStatus(executionStatus)));
    }

    @Override
    public boolean equals(@Nullable Object otherObject) {
        if (this == otherObject) {
            return true;
        } else if (otherObject == null || getClass() != otherObject.getClass()) {
            return false;
        }

        return list.equals(((ExecutionStatusList) otherObject).list);
    }

    @Override
    public int hashCode() {
        return list.hashCode();
    }

    /**
     * Returns the list of active workflow executions, guaranteed not null.
     */
    public List<ExecutionStatus> getList() {
        return list;
    }

    /**
     * Sets the list of active workflow executions.
     *
     * @param list list of active workflow executions
     */
    public ExecutionStatusList setList(List<ExecutionStatus> list) {
        Objects.requireNonNull(list);
        List<ExecutionStatus> backup = new ArrayList<>(list);
        this.list.clear();
        this.list.addAll(backup);
        return this;
    }
}
