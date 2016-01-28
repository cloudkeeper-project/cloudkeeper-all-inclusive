package com.svbio.workflow.base;

import javax.annotation.Nullable;

/**
 * Signals an error while transitioning between lifecycle phases.
 */
public final class LifecycleException extends RuntimeException {
    private static final long serialVersionUID = -6507533140656216132L;

    /**
     * Constructs a new linker exception with the specified detail message.
     *
     * @param message The detail message (which is saved for later retrieval by the {@link #getMessage()} method).
     */
    public LifecycleException(String message) {
        super(message);
    }

    /**
     * Constructs a new linker exception with the specified detail message and cause.
     *
     * @param message The detail message (which is saved for later retrieval by the {@link #getMessage()} method).
     * @param cause The cause (which is saved for later retrieval by the {@link #getCause()} method). A {@code null}
     *     value is permitted, and indicates that the cause is nonexistent or unknown.
     */
    public LifecycleException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
