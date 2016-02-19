package com.svbio.workflow.entities;

import javax.annotation.Nullable;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.util.Date;
import java.util.Objects;

@Entity
public abstract class ProcessLauncherProperties<D extends ProcessLauncherProperties<D>>
        extends ExecutionFrameProperties<D> {
    @Nullable private String commandLine;
    @Nullable private Integer exitStatus;
    private long launcherStartTime = 0;
    private long simpleModuleStartTime = 0;
    private long simpleModuleFinishTime = 0;
    private long launcherFinishTime = 0;

    @Override
    public boolean equals(@Nullable Object otherObject) {
        if (this == otherObject) {
            return true;
        } else if (!super.equals(otherObject)) {
            return false;
        }

        ProcessLauncherProperties<?> other = (ProcessLauncherProperties<?>) otherObject;
        return Objects.equals(commandLine, other.commandLine)
            && Objects.equals(exitStatus, other.exitStatus)
            && launcherStartTime == other.launcherStartTime
            && simpleModuleStartTime == other.simpleModuleStartTime
            && simpleModuleFinishTime == other.simpleModuleFinishTime
            && launcherFinishTime == other.launcherFinishTime;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode()
            + Objects.hash(commandLine, exitStatus, launcherStartTime, simpleModuleStartTime, simpleModuleFinishTime,
                launcherFinishTime);
    }

    /**
     * Returns the command line that was used to start the process executing the simple module.
     *
     * <p>If a {@link xyz.cloudkeeper.executors.ForkingExecutor} was configured, the returned string is the
     * value of property {@link xyz.cloudkeeper.executors.ForkingExecutor#COMMAND_LINE} in the
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutorResult} returned by
     * {@link xyz.cloudkeeper.executors.ForkingExecutor#submit(xyz.cloudkeeper.model.api.RuntimeStateProvider, scala.concurrent.Future)}.
     *
     * <p>If another executor was configured, this field contains a corresponding property.
     */
    @Lob
    @Nullable
    public String getCommandLine() {
        return commandLine;
    }

    public D setCommandLine(@Nullable String commandLine) {
        this.commandLine = commandLine;
        return self();
    }

    /**
     * Returns the exit value of the process that executed the simple module.
     *
     * <p>If a {@link xyz.cloudkeeper.executors.ForkingExecutor} was configured, the returned timestamp is the
     * value of property {@link xyz.cloudkeeper.executors.ForkingExecutor#EXIT_VALUE} in the
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutorResult} returned by
     * {@link xyz.cloudkeeper.executors.ForkingExecutor#submit(xyz.cloudkeeper.model.api.RuntimeStateProvider, scala.concurrent.Future)}.
     *
     * <p>If another executor was configured, this field contains a corresponding property.
     */
    @Nullable
    public Integer getExitStatus() {
        return exitStatus;
    }

    public D setExitStatus(@Nullable Integer exitStatus) {
        this.exitStatus = exitStatus;
        return self();
    }

    /**
     * Returns the timestamp immediately after {@link com.svbio.workflow.forkedexecutor.ForkedExecutor} started.
     *
     * <p>The returned timestamp is the value of property
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutor#SUBMISSION_TIME_MILLIS} in the
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutorResult} written to stdout by
     * {@link com.svbio.workflow.forkedexecutor.ForkedExecutor#main(String[])}.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Nullable
    public Date getLauncherStartTime() {
        return launcherStartTime == 0
            ? null
            : new Date(launcherStartTime);
    }

    public D setLauncherStartTime(@Nullable Date launcherStartTime) {
        this.launcherStartTime = launcherStartTime == null
            ? 0
            : launcherStartTime.getTime();
        return self();
    }

    /**
     * Returns the timestamp immediately before
     * {@link xyz.cloudkeeper.model.api.Executable#run(xyz.cloudkeeper.model.api.ModuleConnector)}
     * was called.
     *
     * <p>The returned timestamp is the value of property
     * {@link xyz.cloudkeeper.simple.LocalSimpleModuleExecutor#PROCESSING_START_TIME_MILLIS} in the
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutorResult} returned by
     * {@link xyz.cloudkeeper.simple.LocalSimpleModuleExecutor#submit(xyz.cloudkeeper.model.api.RuntimeStateProvider, scala.concurrent.Future)}.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Nullable
    public Date getSimpleModuleStartTime() {
        return simpleModuleStartTime == 0
            ? null
            : new Date(simpleModuleStartTime);
    }

    public D setSimpleModuleStartTime(@Nullable Date simpleModuleStartTime) {
        this.simpleModuleStartTime = simpleModuleStartTime == null
            ? 0
            : simpleModuleStartTime.getTime();
        return self();
    }

    /**
     * Returns the timestamp immediately after
     * {@link xyz.cloudkeeper.model.api.Executable#run(xyz.cloudkeeper.model.api.ModuleConnector)}
     * returned.
     *
     * <p>The returned timestamp is the value of property
     * {@link xyz.cloudkeeper.simple.LocalSimpleModuleExecutor#PROCESSING_FINISH_TIME_MILLIS} in the
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutorResult} returned by
     * {@link xyz.cloudkeeper.simple.LocalSimpleModuleExecutor#submit(xyz.cloudkeeper.model.api.RuntimeStateProvider, scala.concurrent.Future)}.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Nullable
    public Date getSimpleModuleFinishTime() {
        return simpleModuleFinishTime == 0
            ? null
            : new Date(simpleModuleFinishTime);
    }

    public D setSimpleModuleFinishTime(@Nullable Date simpleModuleFinishTime) {
        this.simpleModuleFinishTime = simpleModuleFinishTime == null
            ? 0
            : simpleModuleFinishTime.getTime();
        return self();
    }

    /**
     * Returns the timestamp immediately before {@link com.svbio.workflow.forkedexecutor.ForkedExecutor} terminated.
     *
     * <p>The returned timestamp is the value of property
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutor#COMPLETION_TIME_MILLIS} in the
     * {@link xyz.cloudkeeper.model.api.executor.SimpleModuleExecutorResult} written to stdout by
     * {@link com.svbio.workflow.forkedexecutor.ForkedExecutor#main(String[])}.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Nullable
    public Date getLauncherFinishTime() {
        return launcherFinishTime == 0
            ? null
            : new Date(launcherFinishTime);
    }

    public D setLauncherFinishTime(@Nullable Date launcherFinishTime) {
        this.launcherFinishTime = launcherFinishTime == null
            ? 0
            : launcherFinishTime.getTime();
        return self();
    }
}
