package com.svbio.workflow.base;

import com.typesafe.config.Config;
import scala.concurrent.ExecutionContext;

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
     * Returns the {@link ExecutionContext} that is intended for short-lived, ideally non-blocking, asynchronous tasks.
     *
     * @return the {@link ExecutionContext}
     */
    ExecutionContext getExecutionContext();
}
