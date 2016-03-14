package com.svbio.workflow.base;

import com.typesafe.config.Config;
import scala.concurrent.ExecutionContext;

import java.util.concurrent.Executor;

/**
 * Component that provides elementary interfaces that other components depend on.
 *
 * <p>This interface is intended to be used as mix-in interface for Dagger components.
 */
public interface BaseComponent {
    /**
     * Returns the configuration object.
     *
     * @return the {@link Config} instace
     */
    Config getConfig();

    /**
     * Returns the {@link LifecycleManager} that services can register with in order to be informed of lifecycle phase
     * changes.
     *
     * @return the {@link LifecycleManager}
     */
    LifecycleManager getLifecycleManager();

    /**
     * Returns the {@link Executor} that is intended for short-lived, ideally non-blocking, asynchronous tasks.
     *
     * <p>There are no further guarantees for this method. The instances returned by this method and
     * {@link #getExecutionContext()} may represent the same thread pool or they may even be the same.
     *
     * @return the {@link Executor}
     */
    Executor getExecutor();

    /**
     * Returns the {@link ExecutionContext} that is intended for short-lived, ideally non-blocking, asynchronous
     * tasks.
     *
     * <p>There are no further guarantees for this method. The instances returned by this method and
     * {@link #getExecutor()} may represent the same thread pool or they may even be the same.
     *
     * @return the {@link ExecutionContext}
     */
    ExecutionContext getExecutionContext();
}
