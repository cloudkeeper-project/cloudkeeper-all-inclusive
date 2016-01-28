package com.svbio.workflow.base;

/**
 * Phases known to {@link LifecycleManager}.
 */
public enum LifecyclePhase {
    /**
     * The service has been created, but not started yet.
     */
    INITIALIZED,

    /**
     * The service has been started.
     */
    STARTED,

    /**
     * The service has been stopped.
     */
    STOPPED
}
